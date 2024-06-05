# $interpreterPath: path to the python interpreter
# $script: python script content
# $workDir: path to the work directory
# $workName: name of the work
# $checkInterval: time interval for the work to run (in minutes)

$action = New-ScheduledTaskAction -Execute $interpreter -Argument "-c $script" -WorkingDirectory $workDir
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes $checkInterval)

Register-ScheduledTask \
    -TaskName $workName \
    -Action $action \
    -Trigger $trigger | Out-Null

# TODO