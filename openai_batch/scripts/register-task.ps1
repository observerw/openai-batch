# $interpreter: path to the python interpreter
# $script: python script content
# $workDir: path to the work directory
# $taskName: name of the task
# $interval: time interval for the task to run (in seconds)
# $completionWindow: time window for the task to complete (in seconds)

$action = New-ScheduledTaskAction -Execute $interpreter -Argument "-c $script" -WorkingDirectory $workDir
$settings = New-ScheduledTaskSettingsSet -DeleteExpiredTaskAfter (New-TimeSpan -Seconds 2)
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds($interval)
$trigger.EndBoundary = (Get-Date).AddSeconds($completionWindow).ToString("s")

Register-ScheduledTask \
    -TaskName $taskName \
    -Action $action \
    -Settings $settings \
    -Trigger $trigger | Out-Null

# TODO