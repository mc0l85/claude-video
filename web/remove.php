<?php
/**
 * Newtube — Video Removal Endpoint
 *
 * Called by the Python queue worker after successful processing to remove a
 * video from the processing queue (xscribe.txt). Matches lines containing the
 * 11-char YouTube videoID — flags after the URL don't matter.
 *
 * Accepts: POST or GET with 'videoID' parameter (11-char YouTube ID)
 * Returns: JSON response with status and details
 */

header('Content-Type: application/json');

// Update this path to match your server setup.
$filePath = '/var/webs/ng0m/myt/xscribe.txt';

function sendJsonResponse($statusCode, $message, $data = []) {
    http_response_code($statusCode);
    echo json_encode([
        'status_code' => $statusCode,
        'message' => $message,
        'data' => $data
    ]);
    exit;
}

$videoID = null;
if (isset($_REQUEST['videoID'])) {
    $videoID = trim($_REQUEST['videoID']);
}

if (empty($videoID)) {
    sendJsonResponse(400, 'Error: videoID parameter is missing.');
}

if (!preg_match('/^[a-zA-Z0-9_-]{11}$/', $videoID)) {
    sendJsonResponse(400, 'Error: Invalid videoID format. Expected 11 alphanumeric characters.');
}

if (!file_exists($filePath)) {
    sendJsonResponse(500, "Error: Source file not found. Check \$filePath configuration.");
}
if (!is_readable($filePath)) {
    sendJsonResponse(500, "Error: Source file is not readable. Check permissions.");
}
if (!is_writable($filePath)) {
    sendJsonResponse(500, "Error: Source file is not writable. Check permissions.");
}

$fileHandle = fopen($filePath, 'c+');
if (!$fileHandle) {
    sendJsonResponse(500, 'Error: Could not open the file for processing.');
}

if (!flock($fileHandle, LOCK_EX | LOCK_NB, $wouldBlock)) {
    fclose($fileHandle);
    if ($wouldBlock) {
        sendJsonResponse(503, 'Error: File is currently locked by another process. Try again.');
    } else {
        sendJsonResponse(500, 'Error: Could not acquire file lock.');
    }
}

$lines = [];
$originalFileContent = '';
rewind($fileHandle);
while (($lineContent = fgets($fileHandle)) !== false) {
    $lines[] = $lineContent;
    $originalFileContent .= $lineContent;
}

$newLines = [];
$linesRemovedCount = 0;
$videoIDFound = false;

foreach ($lines as $currentLine) {
    if (str_contains($currentLine, $videoID)) {
        $videoIDFound = true;
        $linesRemovedCount++;
    } else {
        $newLines[] = $currentLine;
    }
}

if ($videoIDFound) {
    $newFileContentString = implode('', $newLines);

    if (ftruncate($fileHandle, 0) === false) {
        flock($fileHandle, LOCK_UN);
        fclose($fileHandle);
        sendJsonResponse(500, 'Error: Could not truncate file before writing.');
    }
    rewind($fileHandle);

    if (fwrite($fileHandle, $newFileContentString) === false) {
        ftruncate($fileHandle, 0);
        rewind($fileHandle);
        fwrite($fileHandle, $originalFileContent);

        flock($fileHandle, LOCK_UN);
        fclose($fileHandle);
        sendJsonResponse(500, 'Error: Could not write updated content. Original content restored.');
    } else {
        fflush($fileHandle);
        flock($fileHandle, LOCK_UN);
        fclose($fileHandle);
        sendJsonResponse(200, "Success: Removed {$linesRemovedCount} line(s) containing videoID '{$videoID}'.", [
            'video_id_processed' => $videoID,
            'lines_removed' => $linesRemovedCount
        ]);
    }
} else {
    flock($fileHandle, LOCK_UN);
    fclose($fileHandle);
    sendJsonResponse(200, "Info: No lines containing videoID '{$videoID}' were found.", [
        'video_id_processed' => $videoID,
        'lines_removed' => 0
    ]);
}
?>
