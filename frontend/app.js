/**
 * Snowflake Migration Pipeline - Frontend Application
 * Instructions-based interface with Mermaid diagrams
 */

// Use relative URL - works for both local and deployed
const API_BASE = '';

// State
let currentPhase = 0;
let isRunning = false;
let runFolder = null;

// Initialize Mermaid
mermaid.initialize({
    startOnLoad: false,
    theme: 'base',
    themeVariables: {
        primaryColor: '#BFC9E6',
        primaryTextColor: '#0D1A3A',
        primaryBorderColor: '#0D1A3A',
        lineColor: '#0D1A3A',
        secondaryColor: '#EAE7E0',
        tertiaryColor: '#FFB5A4',
        fontFamily: 'Space Mono, monospace',
    },
    er: {
        diagramPadding: 20,
        layoutDirection: 'TB',
        minEntityWidth: 100,
        minEntityHeight: 75,
        entityPadding: 15,
        useMaxWidth: true,
    },
});

// DOM Elements
const elements = {
    runId: document.getElementById('runId'),
    // Instructions
    phase1Instructions: document.getElementById('phase1Instructions'),
    phase2Instructions: document.getElementById('phase2Instructions'),
    phase3Instructions: document.getElementById('phase3Instructions'),
    alphaModel: document.getElementById('alphaModel'),
    betaModel: document.getElementById('betaModel'),
    debateRounds: document.getElementById('debateRounds'),
    workerModel: document.getElementById('workerModel'),
    workerEffort: document.getElementById('workerEffort'),
    phase1Status: document.getElementById('phase1Status'),
    phase2Status: document.getElementById('phase2Status'),
    phase3Status: document.getElementById('phase3Status'),
    phase1Output: document.getElementById('phase1Output'),
    phase2Output: document.getElementById('phase2Output'),
    phase3Output: document.getElementById('phase3Output'),
    phase1Results: document.getElementById('phase1Results'),
    phase2Results: document.getElementById('phase2Results'),
    phase3Results: document.getElementById('phase3Results'),
    diagramSection: document.getElementById('diagramSection'),
    diagramLoading: document.getElementById('diagramLoading'),
    mermaidDiagram: document.getElementById('mermaidDiagram'),
    startBtn: document.getElementById('startBtn'),
    logSection: document.getElementById('logSection'),
    logContent: document.getElementById('logContent'),
    clearLogBtn: document.getElementById('clearLogBtn'),
    // Progress bar
    progressSection: document.getElementById('progressSection'),
    progressLabel: document.getElementById('progressLabel'),
    progressPercent: document.getElementById('progressPercent'),
    progressFill: document.getElementById('progressFill'),
    progressPhase1: document.getElementById('progressPhase1'),
    progressPhase2: document.getElementById('progressPhase2'),
    progressPhase3: document.getElementById('progressPhase3'),
};

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Generate default run ID
    const timestamp = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    elements.runId.value = `migration-${timestamp}`;
    
    // Event listeners
    elements.startBtn.addEventListener('click', startMigration);
    elements.clearLogBtn.addEventListener('click', clearLog);
});

// Logging
function log(message, type = 'info') {
    elements.logSection.style.display = 'block';
    
    const time = new Date().toLocaleTimeString();
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    
    let typeClass = '';
    if (type === 'phase1') typeClass = 'log-phase1';
    else if (type === 'phase2') typeClass = 'log-phase2';
    else if (type === 'phase3') typeClass = 'log-phase3';
    else if (type === 'success') typeClass = 'log-success';
    else if (type === 'error') typeClass = 'log-error';
    
    entry.innerHTML = `<span class="log-time">[${time}]</span><span class="${typeClass}">${escapeHtml(message)}</span>`;
    elements.logContent.appendChild(entry);
    elements.logContent.scrollTop = elements.logContent.scrollHeight;
}

function clearLog() {
    elements.logContent.innerHTML = '';
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Status updates
function setPhaseStatus(phase, status) {
    const statusEl = elements[`phase${phase}Status`];
    statusEl.textContent = status;
    statusEl.className = 'phase-status';
    
    if (status === 'Running') statusEl.classList.add('running');
    else if (status === 'Complete') statusEl.classList.add('complete');
    else if (status === 'Failed') statusEl.classList.add('error');
}

function showPhaseOutput(phase, content) {
    const outputEl = elements[`phase${phase}Output`];
    const resultsEl = elements[`phase${phase}Results`];
    
    outputEl.style.display = 'block';
    
    // Always use innerHTML for HTML content (tables, etc.)
    resultsEl.innerHTML = content;
}

// Render Mermaid diagram
async function renderMermaidDiagram(mermaidCode) {
    elements.diagramSection.style.display = 'block';
    elements.diagramLoading.style.display = 'none';
    elements.mermaidDiagram.innerHTML = '';
    
    try {
        const { svg } = await mermaid.render('schema-diagram', mermaidCode);
        elements.mermaidDiagram.innerHTML = svg;
    } catch (error) {
        console.error('Mermaid render error:', error);
        elements.mermaidDiagram.innerHTML = `<pre style="color: #666; font-size: 12px;">${escapeHtml(mermaidCode)}</pre>`;
    }
}

function showDiagramLoading() {
    elements.diagramSection.style.display = 'block';
    elements.diagramLoading.style.display = 'flex';
    elements.mermaidDiagram.innerHTML = '';
}

// API Calls
async function apiCall(endpoint, method = 'GET', data = null) {
    const options = {
        method,
        headers: {
            'Content-Type': 'application/json',
        },
    };
    
    if (data) {
        options.body = JSON.stringify(data);
    }
    
    const response = await fetch(`${API_BASE}${endpoint}`, options);
    
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(error.detail || `HTTP ${response.status}`);
    }
    
    return response.json();
}

// Get migration config from instructions
function getMigrationConfig() {
    return {
        run_id: elements.runId.value || `migration-${Date.now()}`,
        phase1_instructions: elements.phase1Instructions.value,
        phase2_instructions: elements.phase2Instructions.value,
        phase3_instructions: elements.phase3Instructions.value,
        planner: {
            alpha_model: elements.alphaModel.value,
            beta_model: elements.betaModel.value,
            debate_rounds: parseInt(elements.debateRounds.value),
        },
        worker: {
            model: elements.workerModel.value,
            effort: elements.workerEffort.value,
        },
    };
}

// Progress bar functions
function showProgress() {
    elements.progressSection.style.display = 'block';
}

function hideProgress() {
    elements.progressSection.style.display = 'none';
}

function updateProgress(percent, label) {
    elements.progressFill.style.width = `${percent}%`;
    elements.progressPercent.textContent = `${percent}%`;
    elements.progressLabel.textContent = label;
}

function setProgressPhase(phase, state) {
    // state: 'pending', 'active', 'complete', 'error'
    const phaseEl = elements[`progressPhase${phase}`];
    phaseEl.className = 'progress-phase';
    if (state !== 'pending') {
        phaseEl.classList.add(state);
    }
}

// Start Migration
async function startMigration() {
    const config = getMigrationConfig();
    
    // Validate required fields
    if (!config.phase1_instructions.trim()) {
        alert('Please provide Phase 1 instructions including connection details.\n\nExample:\nHost: mydb.postgres.database.azure.com\nDatabase: postgres\nSchema: ecommerce\nUsername: admin\nPassword: mypassword');
        elements.phase1Instructions.focus();
        return;
    }
    
    if (!config.run_id) {
        alert('Please enter a run ID');
        return;
    }
    
    isRunning = true;
    elements.startBtn.disabled = true;
    elements.startBtn.innerHTML = '<span class="spinner"></span> Running...';
    
    // Show and reset progress
    showProgress();
    updateProgress(0, 'Initializing...');
    setProgressPhase(1, 'pending');
    setProgressPhase(2, 'pending');
    setProgressPhase(3, 'pending');
    
    clearLog();
    log('Starting E2E Migration Pipeline...', 'info');
    log(`Run ID: ${config.run_id}`, 'info');
    
    try {
        // Start the migration
        const startResult = await apiCall('/api/migration/start', 'POST', config);
        runFolder = startResult.run_folder;
        
        log(`Run folder created: ${runFolder}`, 'info');
        
        // Poll for status updates
        await pollMigrationStatus(startResult.migration_id);
        
    } catch (error) {
        log(`Migration failed: ${error.message}`, 'error');
        setPhaseStatus(currentPhase || 1, 'Failed');
    } finally {
        isRunning = false;
        elements.startBtn.disabled = false;
        elements.startBtn.innerHTML = '<span class="btn-icon">-></span> Start Migration';
    }
}

// Poll for migration status
async function pollMigrationStatus(migrationId) {
    const pollInterval = 2000; // 2 seconds
    let diagramRequested = false;
    
    while (isRunning) {
        try {
            const status = await apiCall(`/api/migration/${migrationId}/status`);
            
            // Update phase statuses
            if (status.phase1) {
                updatePhase1Status(status.phase1);
                
                // Request diagram generation when Phase 1 completes (only once)
                if (status.phase1.status === 'complete' && !diagramRequested) {
                    diagramRequested = true;
                    showDiagramLoading();
                    log('Generating schema diagram...', 'info');
                    
                    // Request diagram in parallel (don't await)
                    apiCall(`/api/migration/${migrationId}/diagram`)
                        .then(diagramResult => {
                            if (diagramResult.mermaid_code) {
                                renderMermaidDiagram(diagramResult.mermaid_code);
                                log('Schema diagram generated', 'success');
                            }
                        })
                        .catch(err => {
                            log(`Diagram generation failed: ${err.message}`, 'error');
                            elements.diagramLoading.style.display = 'none';
                        });
                }
            }
            if (status.phase2) {
                updatePhase2Status(status.phase2);
            }
            if (status.phase3) {
                updatePhase3Status(status.phase3);
            }
            
            // Check if complete
            if (status.complete) {
                if (status.success) {
                    log('Migration completed successfully!', 'success');
                } else {
                    log(`Migration failed: ${status.error}`, 'error');
                }
                break;
            }
            
            // Log any new messages
            if (status.logs) {
                status.logs.forEach(msg => log(msg.message, msg.type));
            }
            
        } catch (error) {
            log(`Status poll error: ${error.message}`, 'error');
        }
        
        await sleep(pollInterval);
    }
}

function updatePhase1Status(phase1) {
    currentPhase = 1;
    
    if (phase1.status === 'running') {
        setPhaseStatus(1, 'Running');
        setProgressPhase(1, 'active');
        const progress = Math.min(5 + (phase1.iteration || 0) * 5, 30);
        updateProgress(progress, `Phase 1: Analyzing schema (iteration ${phase1.iteration || 1})...`);
        if (phase1.iteration) {
            log(`Phase 1: Analyzing schema (iteration ${phase1.iteration})...`, 'phase1');
        }
    } else if (phase1.status === 'complete') {
        setPhaseStatus(1, 'Complete');
        setProgressPhase(1, 'complete');
        updateProgress(33, 'Phase 1 Complete');
        log(`Phase 1: Found ${phase1.tables} tables, ${phase1.relationships} relationships`, 'phase1');
        
        // Show results
        if (phase1.tables_list) {
            const tableHtml = `
                <table class="results-table">
                    <thead>
                        <tr><th>Table</th><th>Rows</th><th>Columns</th></tr>
                    </thead>
                    <tbody>
                        ${phase1.tables_list.map(t => `
                            <tr>
                                <td>${t.name}</td>
                                <td>${t.rows}</td>
                                <td>${t.columns}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            `;
            showPhaseOutput(1, tableHtml);
        }
    } else if (phase1.status === 'failed') {
        setPhaseStatus(1, 'Failed');
        setProgressPhase(1, 'error');
        updateProgress(33, 'Phase 1 Failed');
        log(`Phase 1 failed: ${phase1.error}`, 'error');
    }
}

function updatePhase2Status(phase2) {
    currentPhase = 2;
    
    if (phase2.status === 'running') {
        setPhaseStatus(2, 'Running');
        setProgressPhase(2, 'active');
        const progress = 33 + Math.min((phase2.round || 0) * 5, 30);
        updateProgress(progress, `Phase 2: Debate round ${phase2.round || 1}...`);
        if (phase2.round) {
            log(`Phase 2: Debate round ${phase2.round} - ${phase2.agent || 'processing'}...`, 'phase2');
        } else {
            log('Phase 2: Starting migration planning debate...', 'phase2');
        }
    } else if (phase2.status === 'complete') {
        setPhaseStatus(2, 'Complete');
        setProgressPhase(2, 'complete');
        updateProgress(66, 'Phase 2 Complete');
        log(`Phase 2: Migration plan created after ${phase2.rounds} debate rounds`, 'phase2');
        
        if (phase2.summary) {
            showPhaseOutput(2, phase2.summary);
        }
    } else if (phase2.status === 'failed') {
        setPhaseStatus(2, 'Failed');
        setProgressPhase(2, 'error');
        updateProgress(66, 'Phase 2 Failed');
        log(`Phase 2 failed: ${phase2.error}`, 'error');
    }
}

function updatePhase3Status(phase3) {
    currentPhase = 3;
    
    if (phase3.status === 'running') {
        setPhaseStatus(3, 'Running');
        setProgressPhase(3, 'active');
        const taskProgress = phase3.total ? Math.floor((phase3.completed || 0) / phase3.total * 33) : 0;
        const progress = 66 + taskProgress;
        updateProgress(progress, `Phase 3: ${phase3.task || 'Executing migration'}...`);
        if (phase3.task) {
            log(`Phase 3: Executing ${phase3.task} (attempt ${phase3.attempt || 1})...`, 'phase3');
        } else {
            log('Phase 3: Starting migration execution...', 'phase3');
        }
    } else if (phase3.status === 'complete') {
        setPhaseStatus(3, 'Complete');
        setProgressPhase(3, 'complete');
        updateProgress(100, 'Migration Complete!');
        log(`Phase 3: Migration complete - ${phase3.completed}/${phase3.total} tasks`, 'phase3');
        
        // Show results
        if (phase3.results) {
            const resultHtml = `
                <table class="results-table">
                    <thead>
                        <tr><th>Table</th><th>Source Rows</th><th>Target Rows</th><th>Status</th></tr>
                    </thead>
                    <tbody>
                        ${phase3.results.map(r => `
                            <tr>
                                <td>${r.table}</td>
                                <td>${r.source_rows}</td>
                                <td>${r.target_rows}</td>
                                <td class="${r.match ? 'status-ok' : 'status-fail'}">${r.match ? 'OK' : 'MISMATCH'}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
                <p style="margin-top: 16px;">Duration: ${phase3.duration}s</p>
            `;
            showPhaseOutput(3, resultHtml);
        }
    } else if (phase3.status === 'failed') {
        setPhaseStatus(3, 'Failed');
        setProgressPhase(3, 'error');
        updateProgress(100, 'Migration Failed');
        log(`Phase 3 failed: ${phase3.error}`, 'error');
    }
}

// Utility
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
