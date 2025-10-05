#!/bin/bash
# Setup script for cron job to run daily movie data updates

CRON_COMMAND="/Users/aravetibhanu/projects/virtualenvs/python3.10/bin/python /Users/aravetibhanu/projects/ott-release/ott-release-backend/auto_update.py"

# Cron job: Run daily at 2:00 AM
CRON_TIME="0 2 * * *"

# Full cron entry
CRON_ENTRY="$CRON_TIME $CRON_COMMAND >> /Users/aravetibhanu/projects/ott-release/ott-release-backend/logs/cron.log 2>&1"

echo "Setting up cron job for daily movie data updates..."
echo ""
echo "Cron entry to be added:"
echo "$CRON_ENTRY"
echo ""

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "auto_update.py"; then
    echo "⚠️  Cron job already exists!"
    echo ""
    echo "Current crontab:"
    crontab -l | grep "auto_update.py"
    echo ""
    read -p "Do you want to replace it? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted. No changes made."
        exit 0
    fi
    # Remove existing entry
    crontab -l 2>/dev/null | grep -v "auto_update.py" | crontab -
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

echo ""
echo "✅ Cron job successfully added!"
echo ""
echo "The script will run daily at 2:00 AM"
echo ""
echo "To view your cron jobs, run: crontab -l"
echo "To remove this cron job, run: crontab -e"
echo ""
echo "Logs will be stored in:"
echo "  - Daily logs: /Users/aravetibhanu/projects/ott-release/ott-release-backend/logs/update_YYYYMMDD.log"
echo "  - Cron output: /Users/aravetibhanu/projects/ott-release/ott-release-backend/logs/cron.log"
