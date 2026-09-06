package dev.ccremote.pager.ui

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.FilterChip
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.input.VisualTransformation
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import dev.ccremote.pager.domain.PagerCapability
import dev.ccremote.pager.domain.PagerEngine
import dev.ccremote.pager.domain.PagerLifecycle
import dev.ccremote.pager.domain.PagerSubagentState
import dev.ccremote.pager.domain.PagerTask

@Composable
fun PagerTaskCard(
    task: PagerTask,
    expanded: Boolean,
    onToggleExpanded: () -> Unit,
    onOpenChat: () -> Unit,
    onInterrupt: () -> Unit,
    onAnswer: (String) -> Unit,
    onPin: (Boolean) -> Unit,
    onMarkRead: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val borderColor = when {
        task.lifecycle == PagerLifecycle.WAITING_ANSWER -> PagerColors.Yellow
        task.unread -> PagerColors.Cyan
        task.focused -> MaterialTheme.colorScheme.primary
        else -> MaterialTheme.colorScheme.outlineVariant
    }
    Card(
        modifier = modifier.fillMaxWidth(),
        border = BorderStroke(if (task.focused || task.unread) 1.5.dp else 1.dp, borderColor),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
    ) {
        Column(
            Modifier.clickable {
                onToggleExpanded()
                if (task.unread) onMarkRead()
            }.padding(16.dp),
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                PixelCore(
                    lifecycle = task.lifecycle,
                    activity = task.activity,
                    modifier = Modifier.size(40.dp),
                )
                Spacer(Modifier.width(10.dp))
                Column(Modifier.weight(1f)) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text(
                            if (task.engine == PagerEngine.CODEX) "CODEX" else "CLAUDE",
                            style = MaterialTheme.typography.labelSmall,
                            color = if (task.engine == PagerEngine.CODEX)
                                PagerColors.Cyan else PagerColors.Purple,
                            fontFamily = FontFamily.Monospace,
                            fontWeight = FontWeight.Bold,
                        )
                        Spacer(Modifier.width(8.dp))
                        Text(
                            lifecycleLabel(task.lifecycle),
                            modifier = Modifier.weight(1f),
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            style = MaterialTheme.typography.labelSmall,
                            color = lifecycleColor(task.lifecycle),
                            fontFamily = FontFamily.Monospace,
                        )
                        if (task.unread) {
                            Spacer(Modifier.width(8.dp))
                            Box(Modifier.size(7.dp)) {
                                androidx.compose.foundation.Canvas(Modifier.matchParentSize()) {
                                    drawCircle(PagerColors.Cyan)
                                }
                            }
                        }
                    }
                    Spacer(Modifier.height(5.dp))
                    Text(
                        task.title,
                        style = MaterialTheme.typography.titleMedium,
                        fontWeight = FontWeight.SemiBold,
                        maxLines = if (expanded) 3 else 2,
                        overflow = TextOverflow.Ellipsis,
                    )
                    Text(
                        task.projectName,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                    )
                }
                Text(
                    if (expanded) "−" else "+",
                    modifier = Modifier.padding(start = 10.dp),
                    style = MaterialTheme.typography.headlineSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }

            task.latestStep?.let { step ->
                Spacer(Modifier.height(12.dp))
                Text(
                    step,
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = if (expanded) 4 else 2,
                    overflow = TextOverflow.Ellipsis,
                )
            }

            AnimatedVisibility(expanded) {
                Column {
                    if (task.subagents.isNotEmpty()) {
                        Spacer(Modifier.height(14.dp))
                        Text(
                            "SUBAGENTS · ${task.subagents.size}",
                            style = MaterialTheme.typography.labelMedium,
                            fontFamily = FontFamily.Monospace,
                            color = PagerColors.Muted,
                        )
                        task.subagents.forEach { subagent ->
                            Row(
                                Modifier.fillMaxWidth().padding(top = 7.dp),
                                verticalAlignment = Alignment.CenterVertically,
                            ) {
                                Text(
                                    when (subagent.state) {
                                        PagerSubagentState.RUNNING -> "●"
                                        PagerSubagentState.SUCCEEDED -> "✓"
                                        PagerSubagentState.FAILED -> "!"
                                        PagerSubagentState.INTERRUPTED -> "×"
                                    },
                                    color = when (subagent.state) {
                                        PagerSubagentState.RUNNING -> PagerColors.Green
                                        PagerSubagentState.SUCCEEDED -> PagerColors.Cyan
                                        PagerSubagentState.FAILED,
                                        PagerSubagentState.INTERRUPTED -> PagerColors.Red
                                    },
                                )
                                Spacer(Modifier.width(8.dp))
                                Column {
                                    Text(subagent.title, style = MaterialTheme.typography.bodyMedium)
                                    subagent.latestStep?.let {
                                        Text(
                                            it,
                                            style = MaterialTheme.typography.bodySmall,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                                        )
                                    }
                                }
                            }
                        }
                    }

                    task.question?.let { question ->
                        PagerQuestionPanel(task.id, question, onAnswer)
                    }

                    Spacer(Modifier.height(16.dp))
                    Row(
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        modifier = Modifier.fillMaxWidth(),
                    ) {
                        Button(onClick = onOpenChat, modifier = Modifier.weight(1f)) {
                            Text("打开聊天")
                        }
                        if (PagerCapability.INTERRUPT in task.capabilities) {
                            OutlinedButton(onClick = onInterrupt) { Text("停止") }
                        }
                        if (PagerCapability.PIN in task.capabilities) {
                            FilterChip(
                                selected = task.pinned,
                                onClick = { onPin(!task.pinned) },
                                label = { Text(if (task.pinned) "已置顶" else "置顶") },
                            )
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun PagerQuestionPanel(
    taskId: String,
    question: dev.ccremote.pager.domain.PagerQuestion,
    onAnswer: (String) -> Unit,
) {
    var answer by rememberSaveable(taskId) { mutableStateOf("") }
    Spacer(Modifier.height(16.dp))
    Column(
        Modifier.fillMaxWidth().padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Text(
            question.header ?: "需要你的回答",
            style = MaterialTheme.typography.labelLarge,
            color = PagerColors.Yellow,
        )
        Text(question.question, style = MaterialTheme.typography.bodyMedium)
        if (question.options.isNotEmpty()) {
            Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                question.options.forEach { option ->
                    OutlinedButton(
                        onClick = { onAnswer(option) },
                        modifier = Modifier.fillMaxWidth(),
                    ) { Text(option) }
                }
            }
        }
        if (question.allowText) {
            OutlinedTextField(
                value = answer,
                onValueChange = { answer = it.take(8_192) },
                modifier = Modifier.fillMaxWidth(),
                label = { Text("输入回答") },
                visualTransformation = if (question.secret)
                    PasswordVisualTransformation() else VisualTransformation.None,
                keyboardOptions = KeyboardOptions(
                    keyboardType = if (question.secret)
                        KeyboardType.Password else KeyboardType.Text,
                ),
                singleLine = false,
                maxLines = 4,
            )
            Button(
                onClick = {
                    onAnswer(answer)
                    answer = ""
                },
                enabled = answer.isNotBlank(),
                modifier = Modifier.align(Alignment.End),
            ) { Text("发送") }
        }
    }
}

private fun lifecycleLabel(value: PagerLifecycle): String = when (value) {
    PagerLifecycle.OFFLINE -> "OFFLINE"
    PagerLifecycle.IDLE -> "IDLE"
    PagerLifecycle.STARTING -> "STARTING"
    PagerLifecycle.RUNNING -> "RUNNING"
    PagerLifecycle.WAITING_ANSWER -> "NEEDS INPUT"
    PagerLifecycle.SUCCEEDED -> "DONE"
    PagerLifecycle.INTERRUPTED -> "INTERRUPTED"
}

private fun lifecycleColor(value: PagerLifecycle): Color = when (value) {
    PagerLifecycle.OFFLINE -> PagerColors.Muted
    PagerLifecycle.IDLE -> PagerColors.Cyan
    PagerLifecycle.STARTING -> PagerColors.Purple
    PagerLifecycle.RUNNING -> PagerColors.Green
    PagerLifecycle.WAITING_ANSWER -> PagerColors.Yellow
    PagerLifecycle.SUCCEEDED -> PagerColors.Cyan
    PagerLifecycle.INTERRUPTED -> PagerColors.Red
}
