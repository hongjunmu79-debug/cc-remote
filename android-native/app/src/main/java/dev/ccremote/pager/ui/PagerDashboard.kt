package dev.ccremote.pager.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.selection.selectable
import androidx.compose.foundation.selection.selectableGroup
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.BorderStroke
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Button
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Surface
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.semantics.Role
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.stateDescription
import androidx.compose.ui.unit.dp
import dev.ccremote.pager.PagerUiState
import dev.ccremote.pager.data.ServerEndpoint
import dev.ccremote.pager.domain.PagerEngine
import dev.ccremote.pager.domain.PagerEngineFilter
import dev.ccremote.pager.domain.PagerTask
import dev.ccremote.pager.domain.forEngine

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun PagerDashboard(
    state: PagerUiState,
    onOpenChat: () -> Unit,
    onRefresh: () -> Unit,
    onOpenTask: (PagerTask) -> Unit,
    onInterrupt: (PagerTask) -> Unit,
    onAnswer: (PagerTask, String) -> Unit,
    onPin: (PagerTask, Boolean) -> Unit,
    onMarkRead: (PagerTask) -> Unit,
    onUpdateEndpoint: (String) -> Unit,
    onScanPairing: () -> Unit,
    onFeedbackEnabled: (Boolean) -> Unit,
) {
    var expandedTaskId by rememberSaveable { mutableStateOf<String?>(null) }
    var settingsOpen by rememberSaveable { mutableStateOf(false) }
    var autoRequestedPairing by rememberSaveable { mutableStateOf(false) }
    var engineFilter by rememberSaveable { mutableStateOf(PagerEngineFilter.ALL) }
    val visibleTasks = remember(state.tasks, engineFilter) {
        state.tasks.forEngine(engineFilter)
    }
    val listState = rememberLazyListState()

    LaunchedEffect(engineFilter) {
        expandedTaskId = null
        if (visibleTasks.isNotEmpty()) listState.scrollToItem(0)
    }

    // First launch goes straight to the camera. The normal path is one scan,
    // never a server/IP/password form. If the user cancels or denies camera
    // permission, the unpaired dashboard remains usable and exposes both the
    // scan action and the manual-address recovery path in Settings.
    LaunchedEffect(state.endpoint, state.preferencesLoaded) {
        if (state.preferencesLoaded && state.endpoint == null && !autoRequestedPairing) {
            autoRequestedPairing = true
            onScanPairing()
        }
    }

    Scaffold(
        containerColor = MaterialTheme.colorScheme.background,
        topBar = {
            Column {
            TopAppBar(
                title = {
                    Column {
                        Text(
                            "CC Remote",
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            fontFamily = FontFamily.Monospace,
                            fontWeight = FontWeight.Bold,
                        )
                        Text(
                            state.machineId.ifBlank { "扫码连接电脑" },
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                        )
                    }
                },
            )
            Row(Modifier.fillMaxWidth().padding(horizontal = 8.dp)) {
                TextButton(onClick = onRefresh, modifier = Modifier.weight(1f)) { Text("刷新") }
                TextButton(onClick = onScanPairing, modifier = Modifier.weight(1f)) { Text("扫码") }
                TextButton(onClick = { settingsOpen = true }, modifier = Modifier.weight(1f)) { Text("设置") }
                TextButton(onClick = onOpenChat, modifier = Modifier.weight(1f)) { Text("聊天") }
            }
            }
        },
    ) { padding ->
        Column(
            Modifier.fillMaxSize().padding(padding),
        ) {
            ConnectionBanner(state)
            if (state.tasks.isEmpty()) {
                EmptyDashboard(
                    paired = state.endpoint != null,
                    bridgeConnected = state.bridgeConnected,
                    onOpenChat = onOpenChat,
                    onRefresh = onRefresh,
                    onScanPairing = onScanPairing,
                )
            } else {
                EngineSummary(
                    tasks = state.tasks,
                    selected = engineFilter,
                    onSelected = { engineFilter = it },
                )
                if (visibleTasks.isEmpty()) {
                    FilteredDashboardEmpty(engineFilter)
                } else {
                    LazyColumn(
                        state = listState,
                        modifier = Modifier.fillMaxSize(),
                        contentPadding = androidx.compose.foundation.layout.PaddingValues(
                            start = 14.dp,
                            end = 14.dp,
                            top = 10.dp,
                            bottom = 28.dp,
                        ),
                        verticalArrangement = Arrangement.spacedBy(10.dp),
                    ) {
                        items(visibleTasks, key = PagerTask::id) { task ->
                            PagerTaskCard(
                                task = task,
                                expanded = expandedTaskId == task.id,
                                onToggleExpanded = {
                                    expandedTaskId = if (expandedTaskId == task.id) null else task.id
                                },
                                onOpenChat = { onOpenTask(task) },
                                onInterrupt = { onInterrupt(task) },
                                onAnswer = { onAnswer(task, it) },
                                onPin = { onPin(task, it) },
                                onMarkRead = { onMarkRead(task) },
                            )
                        }
                    }
                }
            }
        }
    }

    if (settingsOpen) {
        PagerSettingsDialog(
            initialUrl = state.endpoint?.url ?: "",
            feedbackEnabled = state.feedbackEnabled,
            onDismiss = { settingsOpen = false },
            onSave = { url ->
                onUpdateEndpoint(url)
                settingsOpen = false
            },
            onScanPairing = {
                settingsOpen = false
                onScanPairing()
            },
            onFeedbackEnabled = onFeedbackEnabled,
        )
    }
}

@Composable
private fun EngineSummary(
    tasks: List<PagerTask>,
    selected: PagerEngineFilter,
    onSelected: (PagerEngineFilter) -> Unit,
) {
    val claudeCount = tasks.count { it.engine == PagerEngine.CLAUDE }
    val codexCount = tasks.count { it.engine == PagerEngine.CODEX }
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 4.dp)
            .selectableGroup(),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        EngineFilterCount(
            label = "全部",
            count = tasks.size,
            color = MaterialTheme.colorScheme.primary,
            selected = selected == PagerEngineFilter.ALL,
            onClick = { onSelected(PagerEngineFilter.ALL) },
            modifier = Modifier.weight(1f),
        )
        EngineFilterCount(
            label = "CLAUDE",
            count = claudeCount,
            color = PagerColors.Green,
            selected = selected == PagerEngineFilter.CLAUDE,
            onClick = { onSelected(PagerEngineFilter.CLAUDE) },
            modifier = Modifier.weight(1f),
        )
        EngineFilterCount(
            label = "CODEX",
            count = codexCount,
            color = PagerColors.Cyan,
            selected = selected == PagerEngineFilter.CODEX,
            onClick = { onSelected(PagerEngineFilter.CODEX) },
            modifier = Modifier.weight(1f),
        )
    }
}

@Composable
private fun EngineFilterCount(
    label: String,
    count: Int,
    color: androidx.compose.ui.graphics.Color,
    selected: Boolean,
    onClick: () -> Unit,
    modifier: Modifier = Modifier,
) {
    Surface(
        modifier = modifier
            .selectable(
                selected = selected,
                role = Role.Tab,
                onClick = onClick,
            )
            .semantics {
                stateDescription = if (selected) "已选择" else "未选择"
            },
        color = if (selected) color.copy(alpha = 0.16f)
        else MaterialTheme.colorScheme.surfaceVariant,
        border = if (selected) BorderStroke(1.dp, color) else null,
        shape = MaterialTheme.shapes.medium,
    ) {
        Column(
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 9.dp),
            horizontalAlignment = Alignment.CenterHorizontally,
        ) {
            Text(
                label,
                color = color,
                fontFamily = FontFamily.Monospace,
                fontWeight = FontWeight.Bold,
                style = MaterialTheme.typography.labelMedium,
                maxLines = 1,
            )
            Text(
                count.toString(),
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.Bold,
            )
        }
    }
}

@Composable
private fun FilteredDashboardEmpty(filter: PagerEngineFilter) {
    val engine = when (filter) {
        PagerEngineFilter.ALL -> "任务"
        PagerEngineFilter.CLAUDE -> "Claude 会话"
        PagerEngineFilter.CODEX -> "Codex 会话"
    }
    Box(
        modifier = Modifier.fillMaxSize().padding(28.dp),
        contentAlignment = Alignment.Center,
    ) {
        Text(
            "当前没有$engine",
            style = MaterialTheme.typography.bodyLarge,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

@Composable
private fun ConnectionBanner(state: PagerUiState) {
    val label = when {
        state.endpoint == null -> "尚未配置服务器 · 请在设置中填写 cc-remote 地址"
        !state.bridgeConnected -> "原生桥已失联 · 网页可能正在登录或重载"
        !state.wrapperOnline -> "电脑端 Wrapper 离线"
        else -> "连接正常 · ${state.tasks.size} 个任务"
    }
    val color = when {
        state.endpoint == null -> PagerColors.Yellow
        !state.bridgeConnected -> PagerColors.Red
        !state.wrapperOnline -> PagerColors.Yellow
        else -> PagerColors.Green
    }
    Row(
        Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        androidx.compose.foundation.Canvas(Modifier.size(18.dp).padding(end = 8.dp)) {
            drawCircle(color, radius = 5.dp.toPx())
        }
        Text(
            label,
            style = MaterialTheme.typography.labelMedium,
            color = color,
            fontFamily = FontFamily.Monospace,
        )
    }
    HorizontalDivider(color = MaterialTheme.colorScheme.outlineVariant)
}

@Composable
private fun EmptyDashboard(
    paired: Boolean,
    bridgeConnected: Boolean,
    onOpenChat: () -> Unit,
    onRefresh: () -> Unit,
    onScanPairing: () -> Unit,
) {
    Box(Modifier.fillMaxSize().padding(28.dp), contentAlignment = Alignment.Center) {
        Column(horizontalAlignment = Alignment.CenterHorizontally) {
            PixelCore(
                lifecycle = if (bridgeConnected)
                    dev.ccremote.pager.domain.PagerLifecycle.IDLE
                else dev.ccremote.pager.domain.PagerLifecycle.OFFLINE,
                activity = null,
                modifier = Modifier.size(120.dp),
            )
            Spacer(Modifier.height(18.dp))
            Text(
                when {
                    !paired -> "扫描电脑二维码即可连接"
                    bridgeConnected -> "当前没有可显示的任务"
                    else -> "正在恢复与电脑的连接"
                },
                style = MaterialTheme.typography.titleMedium,
            )
            Spacer(Modifier.height(8.dp))
            Text(
                when {
                    !paired -> "无需输入 IP 或密码；二维码会自动配置安全会话。"
                    bridgeConnected -> "新建或恢复会话后，看板会自动更新。"
                    else -> "正在自动重连。换网或配对过期时，打开电脑控制台显示新二维码，再点下方重新扫码。"
                },
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(Modifier.height(20.dp))
            if (!paired) {
                Button(onClick = onScanPairing) { Text("扫描配对二维码") }
            } else {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = onOpenChat) { Text("打开聊天") }
                    OutlinedButton(onClick = onRefresh) { Text("重新同步") }
                }
                Spacer(Modifier.height(10.dp))
                OutlinedButton(onClick = onScanPairing) { Text("重新扫码连接电脑") }
            }
        }
    }
}

@Composable
private fun PagerSettingsDialog(
    initialUrl: String,
    feedbackEnabled: Boolean,
    onDismiss: () -> Unit,
    onSave: (String) -> Unit,
    onScanPairing: () -> Unit,
    onFeedbackEnabled: (Boolean) -> Unit,
) {
    var url by rememberSaveable(initialUrl) { mutableStateOf(initialUrl) }
    var pendingHttpUrl by rememberSaveable { mutableStateOf<String?>(null) }
    val parsed = remember(url) { ServerEndpoint.parse(url.trim()).getOrNull() }
    val isCleartextHttp = parsed != null && parsed.url.startsWith("http://")

    if (pendingHttpUrl != null) {
        AlertDialog(
            onDismissRequest = { pendingHttpUrl = null },
            title = { Text("明文 HTTP 警告") },
            text = {
                Text(
                    "该地址使用未加密的 HTTP，任何能监听网络的人都能读取登录凭证和" +
                        "会话内容。仅限在可信局域网内连接私有/本地 IP 时继续。",
                )
            },
            confirmButton = {
                Button(onClick = {
                    val confirm = pendingHttpUrl
                    pendingHttpUrl = null
                    if (confirm != null) onSave(confirm)
                }) { Text("仍然继续") }
            },
            dismissButton = { TextButton(onClick = { pendingHttpUrl = null }) { Text("取消") } },
        )
    }

    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("原生看板设置") },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(14.dp)) {
                Button(
                    onClick = onScanPairing,
                    modifier = Modifier.fillMaxWidth(),
                ) { Text("扫描电脑上的配对二维码") }
                Text(
                    "扫码会自动保存服务器与安全会话，无需输入域名或密码。",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                OutlinedTextField(
                    value = url,
                    onValueChange = { url = it.take(2_048) },
                    label = { Text("cc-remote 地址") },
                    supportingText = {
                        Text(
                            if (isCleartextHttp) {
                                "明文 HTTP：仅限可信局域网的私有/本地 IP，流量不加密"
                            } else {
                                "支持 HTTPS 任意地址；明文 HTTP 仅限私有/本地 IP"
                            },
                        )
                    },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth(),
                )
                Row(
                    Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.SpaceBetween,
                ) {
                    Column(Modifier.weight(1f)) {
                        Text("声音、震动和系统提醒")
                        Text(
                            "任务完成、异常或等待回答时提醒",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                    Switch(
                        checked = feedbackEnabled,
                        onCheckedChange = onFeedbackEnabled,
                    )
                }
            }
        },
        confirmButton = {
            Button(
                onClick = {
                    val trimmed = url.trim()
                    if (trimmed.lowercase().startsWith("http://")) {
                        pendingHttpUrl = trimmed
                    } else {
                        onSave(trimmed)
                    }
                },
                enabled = url.isNotBlank(),
            ) {
                Text("保存并重载")
            }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("取消") } },
    )
}
