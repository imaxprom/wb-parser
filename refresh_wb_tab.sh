#!/bin/bash
# Refresh WB tab in Chrome every 3 hours to keep session-pow-token fresh

echo "[$(date '+%Y-%m-%dT%H:%M:%S')] Refreshing WB tab..."

RESULT=$(osascript -e '
tell application "Google Chrome"
    set found to false
    repeat with w in windows
        set tabList to tabs of w
        repeat with i from 1 to count of tabList
            set t to item i of tabList
            if URL of t contains "wildberries.ru" then
                tell t to reload
                set found to true
                exit repeat
            end if
        end repeat
        if found then exit repeat
    end repeat
    if not found then
        open location "https://www.wildberries.ru/"
    end if
    if found then
        return "reloaded"
    else
        return "opened_new"
    end if
end tell
' 2>&1)

echo "[$(date '+%Y-%m-%dT%H:%M:%S')] Done: $RESULT"
