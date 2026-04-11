# Loam dashboards — PowerShell launcher
#
# Usage (from repo root):
#   . .\scripts\dash.ps1          # load the functions into current session
#   dash sprint                   # one-shot sprint dashboard
#   dash sprint -Watch            # auto-refresh sprint (60s)
#   dash sprint -Watch 30         # auto-refresh sprint (30s)
#   dash sprint -Sprint 30k       # explicit sprint (live or archived)
#   dash project                  # one-shot project/roadmap dashboard
#   dash project -Watch           # auto-refresh project (60s)
#
# Run them side-by-side: open two PowerShell windows (or split a Windows Terminal pane),
# run `dash sprint -Watch` in one, `dash project -Watch` in the other.

function dash {
    [CmdletBinding()]
    param(
        [Parameter(Position = 0, Mandatory = $true)]
        [ValidateSet('sprint', 'project')]
        [string]$View,

        [Alias('W')]
        [int]$Watch,

        [string]$Sprint,

        [switch]$Save
    )

    $pyArgs = @()

    if ($View -eq 'sprint') {
        $module = 'pipeline.analyze.sprint_dashboard'
        if ($Sprint) {
            $pyArgs += @('--sprint', $Sprint)
        }
    }
    else {
        $module = 'pipeline.analyze.loam_roadmap'
    }

    if ($PSBoundParameters.ContainsKey('Watch')) {
        if ($Watch -gt 0) {
            $pyArgs += @('--watch', $Watch)
        }
        else {
            $pyArgs += '--watch'
        }
    }

    if ($Save) {
        $pyArgs += '--save'
    }

    python -m $module @pyArgs
}
