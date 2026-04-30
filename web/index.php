<?php
/**
 * Newtube — Video URL Submission Form
 *
 * Accepts YouTube URLs and adds them to the processing queue (xscribe.txt).
 * Optional flags exposed to the user:
 *   - audio_only: queue worker drops frames before passing to Claude.
 *   - zoom (MM:SS-MM:SS or HH:MM:SS-HH:MM:SS): focused frame extraction window.
 *
 * Line format written to xscribe.txt:
 *   <url>
 *   <url> :audio
 *   <url> :zoom=2:00-2:45
 *   <url> :audio :zoom=2:00-2:45
 */

$fileName = "xscribe.txt";

$message = "";
$message_type = "";

if ($_SERVER["REQUEST_METHOD"] == "POST") {
    if (isset($_POST["video_url"]) && !empty(trim($_POST["video_url"]))) {
        $video_url = trim($_POST["video_url"]);
        $audio_only = isset($_POST["audio_only"]) && $_POST["audio_only"] == "1";
        $zoom = isset($_POST["zoom"]) ? trim($_POST["zoom"]) : "";

        // Validate URL (same set as the queue worker's regex).
        $valid_patterns = [
            '/youtube\.com\/watch\?.*v=[a-zA-Z0-9_-]{11}/',
            '/youtu\.be\/[a-zA-Z0-9_-]{11}/',
            '/youtube\.com\/shorts\/[a-zA-Z0-9_-]{11}/',
            '/youtube\.com\/embed\/[a-zA-Z0-9_-]{11}/',
        ];
        $is_valid_video_url = false;
        foreach ($valid_patterns as $pattern) {
            if (preg_match($pattern, $video_url)) {
                $is_valid_video_url = true;
                break;
            }
        }
        if (!$is_valid_video_url) {
            $message = "Error: Not a valid YouTube video URL. Paste a link like youtube.com/watch?v=... or youtu.be/...";
            $message_type = "error";
        }

        // Validate zoom format (only if user provided a value).
        if ($message_type !== "error" && $zoom !== "") {
            if (!preg_match('/^\d{1,2}:\d{2}(:\d{2})?-\d{1,2}:\d{2}(:\d{2})?$/', $zoom)) {
                $message = "Error: Zoom must be MM:SS-MM:SS or HH:MM:SS-HH:MM:SS (e.g., 2:00-2:45).";
                $message_type = "error";
            }
        }

        // Build the line we'd append.
        $flags = [];
        if ($audio_only) { $flags[] = ":audio"; }
        if ($zoom !== "") { $flags[] = ":zoom=" . $zoom; }
        $line_to_write = $video_url . (empty($flags) ? "" : " " . implode(" ", $flags)) . PHP_EOL;

        // Duplicate check — compare only the URL portion (ignore flags).
        $urlExists = false;
        if ($message_type !== "error" && file_exists($fileName)) {
            $lines = file($fileName, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
            if ($lines === false) {
                $message = "Error: Could not read '" . htmlspecialchars($fileName) . "' to check for duplicates.";
                $message_type = "error";
            } else {
                foreach ($lines as $existing_line) {
                    $existing_url = explode(" ", trim($existing_line))[0];
                    if ($existing_url === $video_url) {
                        $urlExists = true;
                        break;
                    }
                }
            }
        }

        if ($message_type !== "error") {
            if ($urlExists) {
                $message = "Warning: '" . htmlspecialchars($video_url) . "' is already in the queue.";
                $message_type = "warning";
            } else {
                if (file_put_contents($fileName, $line_to_write, FILE_APPEND | LOCK_EX) !== false) {
                    $flag_summary = empty($flags) ? "" : " (" . implode(" ", $flags) . ")";
                    $message = "Added to queue" . htmlspecialchars($flag_summary) . ".";
                    $message_type = "success";
                } else {
                    $message = "Error: Could not write to '" . htmlspecialchars($fileName) . "'. Check server permissions.";
                    $message_type = "error";
                }
            }
        }
    } else {
        $message = "Please enter a video URL.";
        $message_type = "error";
    }
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Newtube — Add Video</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: #1a1a2e;
            color: #e0e0e0;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            margin: 0;
            padding: 20px;
            box-sizing: border-box;
        }
        .container {
            background-color: #24283b;
            padding: 30px 40px;
            border-radius: 12px;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.3);
            width: 100%;
            max-width: 520px;
            text-align: center;
        }
        h1 {
            color: #f0f0f0;
            margin-bottom: 8px;
            font-weight: 300;
        }
        .subtitle {
            color: #888;
            margin-bottom: 25px;
            font-size: 13px;
            border-bottom: 1px solid #4a4e69;
            padding-bottom: 15px;
        }
        .form-group {
            margin-bottom: 18px;
            text-align: left;
        }
        label {
            display: block;
            margin-bottom: 8px;
            font-weight: 500;
            color: #c0c0c0;
        }
        input[type="url"], input[type="text"] {
            width: 100%;
            padding: 12px 15px;
            border: 1px solid #4a4e69;
            border-radius: 6px;
            background-color: #1f2333;
            color: #e0e0e0;
            font-size: 16px;
            box-sizing: border-box;
            transition: border-color 0.3s ease, box-shadow 0.3s ease;
        }
        input[type="url"]:focus, input[type="text"]:focus {
            border-color: #7a7dfa;
            outline: none;
            box-shadow: 0 0 0 3px rgba(122, 125, 250, 0.3);
        }
        .checkbox-row {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .checkbox-row input[type="checkbox"] {
            width: 18px;
            height: 18px;
            accent-color: #7a7dfa;
            margin: 0;
        }
        .checkbox-row label {
            margin: 0;
            font-weight: 400;
            color: #c0c0c0;
            cursor: pointer;
        }
        .hint {
            color: #6b6f8a;
            font-size: 12px;
            margin-top: 4px;
            margin-bottom: 0;
        }
        button[type="submit"] {
            background-color: #7a7dfa;
            color: #ffffff;
            border: none;
            padding: 12px 20px;
            border-radius: 6px;
            font-size: 16px;
            font-weight: 500;
            cursor: pointer;
            transition: background-color 0.3s ease, transform 0.2s ease;
            width: 100%;
            margin-top: 10px;
        }
        button[type="submit"]:hover {
            background-color: #6968e8;
            transform: translateY(-1px);
        }
        button[type="submit"]:active {
            transform: translateY(0px);
        }
        .message {
            padding: 12px;
            margin-top: 20px;
            border-radius: 6px;
            font-size: 14px;
            text-align: center;
        }
        .message.success {
            background-color: rgba(46, 204, 113, 0.15);
            border: 1px solid #2ecc71;
            color: #2ecc71;
        }
        .message.error {
            background-color: rgba(231, 76, 60, 0.15);
            border: 1px solid #e74c3c;
            color: #e74c3c;
        }
        .message.warning {
            background-color: rgba(241, 196, 15, 0.2);
            border: 1px solid #f1c40f;
            color: #f1c40f;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Newtube</h1>
        <div class="subtitle">Drop a YouTube URL into the queue.</div>

        <?php if (!empty($message)): ?>
            <div class="message <?php echo htmlspecialchars($message_type); ?>">
                <?php echo $message; ?>
            </div>
        <?php endif; ?>

        <form action="<?php echo htmlspecialchars($_SERVER["PHP_SELF"]); ?>" method="POST">
            <div class="form-group">
                <label for="video_url">YouTube URL:</label>
                <input type="text" id="video_url" name="video_url" placeholder="https://youtube.com/watch?v=..." required>
            </div>

            <div class="form-group checkbox-row">
                <input type="checkbox" id="audio_only" name="audio_only" value="1">
                <label for="audio_only">Audio only (skip frames — for podcasts &amp; talking heads)</label>
            </div>

            <div class="form-group">
                <label for="zoom">Zoom (optional):</label>
                <input type="text" id="zoom" name="zoom" placeholder="MM:SS-MM:SS, e.g., 2:00-2:45">
                <p class="hint">Frames extracted only from this window. Leave blank for full video.</p>
            </div>

            <button type="submit">Add to Queue</button>
        </form>
    </div>
</body>
</html>
