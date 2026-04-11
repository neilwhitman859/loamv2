# scripts/dash.ps1 - Loam sprint/session progress dashboard (native PowerShell)
#
# Fresh PowerShell-native dashboard for tracking Loam sprint and session progress.
# Reads data/sprints/current.json, live sprint directories, and _archive/ directly.
# No Python dependency for the sprint view. The project/roadmap view still shells out
# to pipeline.analyze.loam_roadmap for the live DB metrics.
#
# Session naming: S{sprint}.{session} (e.g. S2.1 = Sprint 2 Session 1). Sprint number
# comes from meta.json "sprint_number" field if present, otherwise from a known-name
# fallback map (30k=1, audit=2, execute=3, reference_design=4, reference_enrichment=5).
#
# Usage (dot-source first, from repo root):
#   . .\scripts\dash.ps1
#   dash                 # default view: active sprint, or between-sprints summary
#   dash -All            # multi-sprint overview (all live + archived)
#   dash -Sprint 30k     # focused view of one sprint (live or archived)
#   dash -Watch          # auto-refresh default view every 60s
#   dash -Watch 30       # auto-refresh every 30s
#   dash project         # fall through to python -m pipeline.analyze.loam_roadmap
#
# Direct run (no dot-source):
#   powershell -File .\scripts\dash.ps1
#   powershell -File .\scripts\dash.ps1 -All
#   powershell -File .\scripts\dash.ps1 -Sprint 30k

$script:LoamRoot    = Split-Path $PSScriptRoot -Parent
$script:SprintsRoot = Join-Path $script:LoamRoot 'data/sprints'

# ============================================================
# File readers
# ============================================================

function Read-JsonSafe {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    try {
        Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        $null
    }
}

function Read-TextSafe {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return '' }
    try {
        Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    } catch {
        ''
    }
}

function Get-SprintBundle {
    param([string]$SprintPath)

    $meta     = Read-JsonSafe (Join-Path $SprintPath 'meta.json')
    $sessData = Read-JsonSafe (Join-Path $SprintPath 'sessions.json')
    $budget   = Read-JsonSafe (Join-Path $SprintPath 'budget.json')
    $status   = Read-TextSafe (Join-Path $SprintPath 'status.md')

    $sessionList = @()
    if ($sessData -and $sessData.PSObject.Properties['sessions']) {
        $sessionList = @($sessData.sessions)
    } elseif ($sessData -is [System.Array]) {
        $sessionList = @($sessData)
    }

    [PSCustomObject]@{
        Path      = $SprintPath
        Name      = Split-Path $SprintPath -Leaf
        IsArchive = $SprintPath -like '*_archive*'
        Meta      = $meta
        Sessions  = $sessionList
        Budget    = $budget
        Status    = $status
    }
}

function Get-CurrentPointer {
    Read-JsonSafe (Join-Path $script:SprintsRoot 'current.json')
}

function Get-AllSprints {
    $sprints = @()

    if (Test-Path $script:SprintsRoot) {
        Get-ChildItem -Path $script:SprintsRoot -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -ne '_archive' } |
            ForEach-Object {
                $bundle = Get-SprintBundle $_.FullName
                if ($bundle.Meta) { $sprints += $bundle }
            }
    }

    $archiveRoot = Join-Path $script:SprintsRoot '_archive'
    if (Test-Path $archiveRoot) {
        Get-ChildItem -Path $archiveRoot -Directory -ErrorAction SilentlyContinue |
            ForEach-Object {
                $bundle = Get-SprintBundle $_.FullName
                if ($bundle.Meta) { $sprints += $bundle }
            }
    }

    , $sprints
}

function Find-SprintByName {
    param([string]$Name)
    $live = Join-Path $script:SprintsRoot $Name
    if (Test-Path $live) { return Get-SprintBundle $live }
    $archived = Join-Path (Join-Path $script:SprintsRoot '_archive') $Name
    if (Test-Path $archived) { return Get-SprintBundle $archived }
    $null
}

function Get-ActiveSprint {
    $current = Get-CurrentPointer
    if (-not $current) { return $null }
    if ($current.status -eq 'between_sprints') { return $null }
    if (-not $current.name) { return $null }
    Find-SprintByName $current.name
}

# ============================================================
# Formatters
# ============================================================

function Format-ProgressBar {
    param(
        [double]$Current,
        [double]$Total,
        [int]$Width = 24
    )
    if (-not $Total -or $Total -le 0) {
        return ('[' + ('.' * $Width) + ']   -%')
    }
    $ratio = $Current / $Total
    if ($ratio -lt 0) { $ratio = 0 }
    if ($ratio -gt 1) { $ratio = 1 }
    $filled = [int][Math]::Round($ratio * $Width)
    $empty  = $Width - $filled
    $pct    = [int][Math]::Round($ratio * 100)
    '[' + ('#' * $filled) + ('.' * $empty) + '] ' + ('{0,3}%' -f $pct)
}

function Get-StatusColor {
    param([string]$Status)
    if (-not $Status) { return 'Gray' }
    switch -Regex ($Status.ToLower()) {
        '^(done|closed|completed|archived)' { 'Green' }
        '^(in_progress|active|open)$'       { 'Yellow' }
        '^(blocked|failed|error)$'          { 'Red' }
        '^(planning|pending|not_started|future|between_sprints)$' { 'DarkGray' }
        default { 'Gray' }
    }
}

function Get-StatusIcon {
    param([string]$Status)
    if (-not $Status) { return '[ ]' }
    switch -Regex ($Status.ToLower()) {
        '^(done|closed|completed|archived)' { '[x]' }
        '^(in_progress|active|open)$'       { '[>]' }
        '^(blocked|failed|error)$'          { '[!]' }
        '^(planning|pending|not_started|future)$' { '[ ]' }
        'between_sprints' { '[=]' }
        default { '[ ]' }
    }
}

# Sprint number resolver: meta.json sprint_number field, then a known-name fallback.
# Returns 0 if unknown (dashboard falls back to plain S{n} session labels).
function Get-SprintNumber {
    param([PSCustomObject]$Bundle)
    if ($Bundle -and $Bundle.Meta -and $Bundle.Meta.PSObject.Properties['sprint_number']) {
        return [int]$Bundle.Meta.sprint_number
    }
    $name = if ($Bundle -and $Bundle.Name) { $Bundle.Name.ToLower() } else { '' }
    switch ($name) {
        '30k'                   { 1 }
        'audit'                 { 2 }
        'reference_first'       { 2 }
        'execute'               { 3 }
        'reference_design'      { 4 }
        'reference_enrichment'  { 5 }
        default                 { 0 }
    }
}

function Get-SprintNumberByName {
    param([string]$Name)
    if (-not $Name) { return 0 }
    switch ($Name.ToLower()) {
        '30k'                   { 1 }
        'audit'                 { 2 }
        'reference_first'       { 2 }
        'execute'               { 3 }
        'reference_design'      { 4 }
        'reference_enrichment'  { 5 }
        default                 { 0 }
    }
}

function Format-SessionId {
    param(
        [int]$SprintNumber,
        [int]$SessionNumber
    )
    if ($SprintNumber -gt 0) {
        'S{0}.{1}' -f $SprintNumber, $SessionNumber
    } else {
        'S{0}' -f $SessionNumber
    }
}

# ============================================================
# Rendering primitives
# ============================================================

$script:DoubleRule = '=' * 65
$script:SingleRule = '  ' + ('-' * 61)

function Write-Dash-Header {
    param([string]$Title)
    $now = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Write-Host $script:DoubleRule -ForegroundColor Cyan
    $left = "  $Title".PadRight(45)
    Write-Host ($left + $now) -ForegroundColor Cyan
    Write-Host $script:DoubleRule -ForegroundColor Cyan
}

function Write-Dash-Footer {
    Write-Host $script:DoubleRule -ForegroundColor Cyan
}

function Write-Dash-Divider {
    Write-Host $script:SingleRule -ForegroundColor DarkGray
}

function Write-Dash-SectionTitle {
    param([string]$Title, [string]$Note = '')
    if ($Note) {
        Write-Host "  $Title" -ForegroundColor White -NoNewline
        Write-Host "  ($Note)" -ForegroundColor DarkGray
    } else {
        Write-Host "  $Title" -ForegroundColor White
    }
    Write-Dash-Divider
}

function Show-SprintSummary {
    param([PSCustomObject]$Bundle)

    $meta = $Bundle.Meta
    if (-not $meta) { return }

    $sprintNum = Get-SprintNumber $Bundle
    $name = if ($meta.display_name) { $meta.display_name } else { $meta.name }
    $status = [string]$meta.status
    if ($Bundle.IsArchive -and $status -notmatch 'archived') {
        $status = "archived ($status)"
    }

    $color = Get-StatusColor $status
    $icon  = Get-StatusIcon $status

    $prefix = if ($sprintNum -gt 0) { "Sprint $sprintNum" } else { 'Sprint' }

    Write-Host ''
    Write-Host "  $icon $prefix - " -ForegroundColor White -NoNewline
    Write-Host $name -ForegroundColor White -NoNewline
    Write-Host "  [$status]" -ForegroundColor $color

    # Sessions progress
    $sessions = $Bundle.Sessions
    if ($sessions -and $sessions.Count -gt 0) {
        $total      = $sessions.Count
        $done       = @($sessions | Where-Object { $_.status -eq 'done' }).Count
        $inProgress = @($sessions | Where-Object { $_.status -eq 'in_progress' }).Count

        $bar  = Format-ProgressBar -Current $done -Total $total -Width 24
        $line = '  Sessions  {0}  {1} / {2}' -f $bar, $done, $total
        if ($inProgress -gt 0) {
            $line += ' ({0} active)' -f $inProgress
        }
        Write-Host $line -ForegroundColor Gray
    }

    # Budget
    $budget = $Bundle.Budget
    if ($budget) {
        $spent = if ($null -ne $budget.total_spent) { [double]$budget.total_spent } else { 0.0 }
        $cap   = if ($null -ne $budget.total_budget) { [double]$budget.total_budget } else { 0.0 }
        if ($cap -gt 0) {
            $bar  = Format-ProgressBar -Current $spent -Total $cap -Width 24
            $line = '  Budget    {0}  ${1:N2} / ${2:N2}' -f $bar, $spent, $cap
            Write-Host $line -ForegroundColor Gray
        }
    }
}

function Show-SessionList {
    param([PSCustomObject]$Bundle)

    $sessions = $Bundle.Sessions
    if (-not $sessions -or $sessions.Count -eq 0) { return }

    $sprintNum = Get-SprintNumber $Bundle

    Write-Host ''
    Write-Dash-SectionTitle -Title 'SESSIONS'

    foreach ($s in $sessions) {
        $sid = if ($null -ne $s.id) { [int]$s.id } else { 0 }
        $label = Format-SessionId -SprintNumber $sprintNum -SessionNumber $sid

        $sname = if ($s.name) { [string]$s.name } else { '' }
        if ($sname.Length -gt 44) { $sname = $sname.Substring(0, 42) + '..' }

        $sstatus = if ($s.status) { [string]$s.status } else { 'not_started' }
        $sicon   = Get-StatusIcon $sstatus
        $scolor  = Get-StatusColor $sstatus

        $sdate = if ($s.date) { [string]$s.date } else { '          ' }

        if ($null -ne $s.ai_spend) {
            $spendVal = [double]$s.ai_spend
            $sspend = '${0,6:N2}' -f $spendVal
        } else {
            $sspend = '      -'
        }

        $line = '  {0} {1,-6} {2,-44} {3,-10} {4}' -f $sicon, $label, $sname, $sdate, $sspend
        Write-Host $line -ForegroundColor $scolor
    }
}

# ============================================================
# Dashboard views
# ============================================================

function Show-DashboardDefault {
    Write-Dash-Header 'LOAM - SPRINT DASHBOARD'

    $current = Get-CurrentPointer
    $active  = Get-ActiveSprint

    if ($active) {
        Show-SprintSummary -Bundle $active
        Show-SessionList  -Bundle $active
    }
    elseif ($current -and $current.status -eq 'between_sprints') {
        Write-Host ''
        Write-Host '  BETWEEN SPRINTS' -ForegroundColor Yellow

        if ($current.previous -and $current.previous.name) {
            $prevBundle = Find-SprintByName $current.previous.name
            if ($prevBundle) {
                Write-Host ''
                Write-Host '  JUST CLOSED' -ForegroundColor DarkGray
                Show-SprintSummary -Bundle $prevBundle
                if ($prevBundle.Meta.closure_reason) {
                    $reason = [string]$prevBundle.Meta.closure_reason
                    if ($reason.Length -gt 260) { $reason = $reason.Substring(0, 257) + '...' }
                    Write-Host ''
                    Write-Host '  Closure note:' -ForegroundColor DarkGray
                    Write-Host "  $reason" -ForegroundColor Gray
                }
            }
        }

        if ($current.next -and $current.next.name) {
            $next = $current.next
            Write-Host ''
            Write-Host '  UP NEXT' -ForegroundColor DarkGray
            Write-Host ''
            $nextStatus = if ($next.status) { [string]$next.status } else { 'planned' }
            $color = Get-StatusColor $nextStatus
            Write-Host "  [+] $($next.name)" -ForegroundColor White -NoNewline
            Write-Host "  [$nextStatus]" -ForegroundColor $color
            if ($next.planning_session) {
                Write-Host "  Planning session: $($next.planning_session)" -ForegroundColor DarkGray
            }
            if ($next.notes) {
                $notes = [string]$next.notes
                if ($notes.Length -gt 220) { $notes = $notes.Substring(0, 217) + '...' }
                Write-Host "  $notes" -ForegroundColor Gray
            }
        }

        if ($current.notes) {
            Write-Host ''
            $cn = [string]$current.notes
            if ($cn.Length -gt 220) { $cn = $cn.Substring(0, 217) + '...' }
            Write-Host "  $cn" -ForegroundColor DarkGray
        }
    }
    else {
        Write-Host ''
        if ($current -and $current.name) {
            Write-Host "  Sprint '$($current.name)' pointed at by current.json but not found on disk." -ForegroundColor Red
            Write-Host "  Check data/sprints/$($current.name)/" -ForegroundColor DarkGray
        } else {
            Write-Host '  No sprint state found.' -ForegroundColor Red
            Write-Host '  Expected data/sprints/current.json' -ForegroundColor DarkGray
        }
    }

    Write-Host ''
    Write-Dash-Footer
}

function Show-DashboardAll {
    Write-Dash-Header 'LOAM - ALL SPRINTS'

    $sprints = Get-AllSprints
    $current = Get-CurrentPointer

    if ((-not $sprints -or $sprints.Count -eq 0) -and -not $current) {
        Write-Host ''
        Write-Host '  No sprints found.' -ForegroundColor Red
        Write-Host ''
        Write-Dash-Footer
        return
    }

    # Sort by sprint_number ascending, then by name
    $sorted = @($sprints | Sort-Object `
        @{ Expression = { Get-SprintNumber $_ } }, `
        @{ Expression = { $_.Name } })

    Write-Host ''
    Write-Dash-SectionTitle -Title 'KNOWN SPRINTS'
    $header = '  {0,-4} {1,-28} {2,-14} {3,-10} {4}' -f '#', 'Name', 'Status', 'Sessions', 'Budget'
    Write-Host $header -ForegroundColor DarkGray

    $totalSpent    = 0.0
    $totalBudget   = 0.0
    $totalDone     = 0
    $totalSessions = 0

    foreach ($bundle in $sorted) {
        $meta = $bundle.Meta
        if (-not $meta) { continue }

        $num    = Get-SprintNumber $bundle
        $numStr = if ($num -gt 0) { "S$num" } else { '--' }

        $name = if ($meta.display_name) { $meta.display_name } else { $meta.name }
        $name = [string]$name
        if ($name.Length -gt 28) { $name = $name.Substring(0, 26) + '..' }

        $status = [string]$meta.status
        if ($bundle.IsArchive -and $status -notmatch 'archived') {
            $status = 'archived'
        }
        if ($status.Length -gt 14) { $status = $status.Substring(0, 14) }

        $sessions = $bundle.Sessions
        $done = 0; $total = 0
        if ($sessions) {
            $done  = @($sessions | Where-Object { $_.status -eq 'done' }).Count
            $total = $sessions.Count
        }
        $sessionsStr = '{0}/{1}' -f $done, $total

        $budget = $bundle.Budget
        $budgetStr = '-'
        if ($budget -and $budget.total_budget) {
            $spent = if ($null -ne $budget.total_spent) { [double]$budget.total_spent } else { 0.0 }
            $cap   = [double]$budget.total_budget
            $budgetStr = '${0:N0}/${1:N0}' -f $spent, $cap
            $totalSpent  += $spent
            $totalBudget += $cap
        }
        $totalDone     += $done
        $totalSessions += $total

        $color = Get-StatusColor $status
        $icon  = Get-StatusIcon $status
        $line  = '  {0} {1,-2} {2,-28} {3,-14} {4,-10} {5}' -f $icon, $numStr, $name, $status, $sessionsStr, $budgetStr
        Write-Host $line -ForegroundColor $color
    }

    # Append "up next" from current.json if we're between sprints
    if ($current -and $current.status -eq 'between_sprints' -and $current.next -and $current.next.name) {
        $next = $current.next
        $nextStatus = if ($next.status) { [string]$next.status } else { 'planned' }
        if ($nextStatus.Length -gt 14) { $nextStatus = $nextStatus.Substring(0, 14) }
        $nextNum = Get-SprintNumberByName $next.name
        $numStr  = if ($nextNum -gt 0) { "S$nextNum" } else { '--' }
        $nextName = [string]$next.name
        if ($nextName.Length -gt 28) { $nextName = $nextName.Substring(0, 26) + '..' }
        $line = '  {0} {1,-2} {2,-28} {3,-14} {4,-10} {5}' -f '[+]', $numStr, $nextName, $nextStatus, '-', '-'
        Write-Host $line -ForegroundColor Cyan
    }

    Write-Host ''
    Write-Dash-SectionTitle -Title 'TOTAL'
    $sessionLine = '  {0} / {1} sessions done' -f $totalDone, $totalSessions
    Write-Host $sessionLine -ForegroundColor Gray
    if ($totalBudget -gt 0) {
        $budgetLine = '  ${0:N2} spent / ${1:N2} budgeted' -f $totalSpent, $totalBudget
        Write-Host $budgetLine -ForegroundColor Gray
    }

    Write-Host ''
    Write-Dash-Footer
}

function Show-DashboardSingle {
    param([string]$Name)

    $bundle = Find-SprintByName $Name
    if (-not $bundle) {
        Write-Dash-Header "LOAM - SPRINT $Name"
        Write-Host ''
        Write-Host "  Sprint '$Name' not found." -ForegroundColor Red
        Write-Host '  Looked in:' -ForegroundColor DarkGray
        Write-Host "    data/sprints/$Name/" -ForegroundColor DarkGray
        Write-Host "    data/sprints/_archive/$Name/" -ForegroundColor DarkGray
        Write-Host ''
        Write-Dash-Footer
        return
    }

    $num = Get-SprintNumber $bundle
    $title = if ($num -gt 0) { "LOAM - SPRINT $num" } else { "LOAM - SPRINT $Name" }
    Write-Dash-Header $title

    Show-SprintSummary -Bundle $bundle
    Show-SessionList  -Bundle $bundle

    Write-Host ''
    Write-Dash-Footer
}

# ============================================================
# Entry point
# ============================================================

function dash {
    [CmdletBinding()]
    param(
        [Parameter(Position = 0)]
        [string]$View = '',

        [Alias('W')]
        [int]$Watch,

        [string]$Sprint,

        [switch]$All
    )

    # dash project -> delegate to the Python roadmap dashboard
    if ($View -eq 'project') {
        $pyArgs = @('-m', 'pipeline.analyze.loam_roadmap')
        if ($PSBoundParameters.ContainsKey('Watch')) {
            if ($Watch -gt 0) {
                $pyArgs += @('--watch', "$Watch")
            } else {
                $pyArgs += '--watch'
            }
        }
        & python @pyArgs
        return
    }

    # Everything else (dash, dash sprint, dash -All, dash -Sprint <name>) uses the native view.
    # Inline dispatch (no scriptblock — scriptblock scoping eats parent $Sprint in some contexts).

    if ($PSBoundParameters.ContainsKey('Watch')) {
        $interval = if ($Watch -gt 0) { $Watch } else { 60 }
        try {
            while ($true) {
                Clear-Host
                if ($All) {
                    Show-DashboardAll
                } elseif ($Sprint) {
                    Show-DashboardSingle -Name $Sprint
                } else {
                    Show-DashboardDefault
                }
                Write-Host ''
                Write-Host "  Refreshing every ${interval}s... (Ctrl+C to stop)" -ForegroundColor DarkGray
                Start-Sleep -Seconds $interval
            }
        } catch {
            Write-Host ''
            Write-Host '  Stopped.' -ForegroundColor DarkGray
        }
    } else {
        if ($All) {
            Show-DashboardAll
        } elseif ($Sprint) {
            Show-DashboardSingle -Name $Sprint
        } else {
            Show-DashboardDefault
        }
    }
}

# Auto-run when executed directly (not dot-sourced).
# Dot-sourcing sets $MyInvocation.InvocationName to '.'; direct runs use the script name.
if ($MyInvocation.InvocationName -ne '.') {
    dash @args
}
