/**
 * DART Web UI Client Logic
 * Handles interactive controls, real-time packet animation, sliding window tape,
 * and live telemetry updates from the UDP socket engine.
 */

document.addEventListener("DOMContentLoaded", () => {
    // State
    const state = {
        arq: "selective_repeat",
        mode: "dart",
        payloadKb: 50,
        lossRate: 0.10,
        delayMs: 15,
        jitterMs: 5,
        reorderRate: 0.08,
        corruptRate: 0.0,
        dupRate: 0.0,
        seed: 42,
        isTransferring: false
    };

    // DOM Elements
    const btnSaw = document.getElementById("btnSaw");
    const btnGbn = document.getElementById("btnGbn");
    const btnSr = document.getElementById("btnSr");
    const btnModeP3 = document.getElementById("btnModeP3");
    const btnModeDart = document.getElementById("btnModeDart");
    
    const activeArqLabel = document.getElementById("activeArqLabel");
    const activeModeLabel = document.getElementById("activeModeLabel");

    // Sliders
    const lossSlider = document.getElementById("lossSlider");
    const lossVal = document.getElementById("lossVal");
    const delaySlider = document.getElementById("delaySlider");
    const delayVal = document.getElementById("delayVal");
    const jitterSlider = document.getElementById("jitterSlider");
    const jitterVal = document.getElementById("jitterVal");
    const reorderSlider = document.getElementById("reorderSlider");
    const reorderVal = document.getElementById("reorderVal");
    const corruptSlider = document.getElementById("corruptSlider");
    const corruptVal = document.getElementById("corruptVal");
    const dupSlider = document.getElementById("dupSlider");
    const dupVal = document.getElementById("dupVal");

    const launchBtn = document.getElementById("launchBtn");
    const progressFill = document.getElementById("progressFill");
    const progressPct = document.getElementById("progressPct");
    const transferredBytesLabel = document.getElementById("transferredBytesLabel");
    const rateLabel = document.getElementById("rateLabel");
    
    const windowTape = document.getElementById("windowTape");
    const windowDimBadge = document.getElementById("windowDimBadge");
    const fsmBadge = document.getElementById("fsmBadge");
    const fsmStateName = document.getElementById("fsmStateName");
    const packetRail = document.getElementById("packetRail");
    const eventsFeed = document.getElementById("eventsFeed");
    const eventCountBadge = document.getElementById("eventCountBadge");

    // Telemetry Elements
    const metricGoodput = document.getElementById("metricGoodput");
    const metricGoodputMbps = document.getElementById("metricGoodputMbps");
    const metricRtt = document.getElementById("metricRtt");
    const metricRto = document.getElementById("metricRto");
    const metricOverhead = document.getElementById("metricOverhead");
    const metricRetransCount = document.getElementById("metricRetransCount");
    const metricTimeouts = document.getElementById("metricTimeouts");

    // Integrity Elements
    const srcShaLabel = document.getElementById("srcShaLabel");
    const dstShaLabel = document.getElementById("dstShaLabel");
    const integrityBadge = document.getElementById("integrityBadge");
    const integrityStatus = document.getElementById("integrityStatus");

    // Plot Modal Elements
    const plotModal = document.getElementById("plotModal");
    const modalPlotImg = document.getElementById("modalPlotImg");
    const modalPlotTitle = document.getElementById("modalPlotTitle");
    const modalCloseBtn = document.getElementById("modalCloseBtn");

    let eventCount = 0;

    // --- ARQ Switcher ---
    const arqButtons = [btnSaw, btnGbn, btnSr];
    arqButtons.forEach(btn => {
        btn.addEventListener("click", () => {
            arqButtons.forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            state.arq = btn.dataset.arq;
            
            const labels = {
                "stop_and_wait": "Stop-and-Wait",
                "go_back_n": "Go-Back-N",
                "selective_repeat": "Selective Repeat"
            };
            activeArqLabel.textContent = labels[state.arq];
            initWindowTape();
            logEvent("system", `ARQ switched to ${labels[state.arq]}`);
        });
    });

    // --- Mode Switcher ---
    btnModeP3.addEventListener("click", () => {
        btnModeP3.classList.add("active");
        btnModeDart.classList.remove("active");
        state.mode = "p3";
        activeModeLabel.textContent = "Static P3";
        updateFsmState("STATIC_P3");
        logEvent("system", "Strategy mode: Static P3 Baseline (Fixed Parameters)");
    });

    btnModeDart.addEventListener("click", () => {
        btnModeDart.classList.add("active");
        btnModeP3.classList.remove("active");
        state.mode = "dart";
        activeModeLabel.textContent = "DART Adaptive";
        updateFsmState("STARTUP");
        logEvent("system", "Strategy mode: DART Dynamic Adaptive Engine (Active FSM)");
    });

    // --- Slider Bindings ---
    lossSlider.addEventListener("input", (e) => {
        state.lossRate = parseFloat(e.target.value) / 100.0;
        lossVal.textContent = `${e.target.value}.0%`;
    });

    delaySlider.addEventListener("input", (e) => {
        state.delayMs = parseInt(e.target.value);
        delayVal.textContent = `${state.delayMs} ms`;
    });

    jitterSlider.addEventListener("input", (e) => {
        state.jitterMs = parseInt(e.target.value);
        jitterVal.textContent = `${state.jitterMs} ms`;
    });

    reorderSlider.addEventListener("input", (e) => {
        state.reorderRate = parseFloat(e.target.value) / 100.0;
        reorderVal.textContent = `${e.target.value}.0%`;
    });

    corruptSlider.addEventListener("input", (e) => {
        state.corruptRate = parseFloat(e.target.value) / 100.0;
        corruptVal.textContent = `${e.target.value}.0%`;
    });

    dupSlider.addEventListener("input", (e) => {
        state.dupRate = parseFloat(e.target.value) / 100.0;
        dupVal.textContent = `${e.target.value}.0%`;
    });

    // --- Payload Presets ---
    document.querySelectorAll(".preset-btn").forEach(btn => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".preset-btn").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            state.payloadKb = parseInt(btn.dataset.kb);
            transferredBytesLabel.textContent = `0 / ${state.payloadKb} KB`;
            initWindowTape();
        });
    });

    // --- Window Tape Initialization ---
    function initWindowTape(totalSlots = 35) {
        windowTape.innerHTML = "";
        const windowSize = state.arq === "stop_and_wait" ? 1 : (state.mode === "dart" ? 4 : 4);
        windowDimBadge.textContent = `Window: W = ${windowSize}`;

        for (let i = 0; i < totalSlots; i++) {
            const cell = document.createElement("div");
            cell.className = "pkt-cell";
            cell.id = `pkt-cell-${i}`;
            cell.textContent = i;
            if (i < windowSize) {
                cell.classList.add("in-window");
            }
            windowTape.appendChild(cell);
        }
    }

    function updateWindowTape(base, windowSize, ackedSet = new Set()) {
        windowDimBadge.textContent = `Window: W = ${windowSize} (Base: ${base})`;
        const cells = windowTape.querySelectorAll(".pkt-cell");
        cells.forEach((cell, idx) => {
            cell.className = "pkt-cell";
            if (idx >= base && idx < (base + windowSize)) {
                cell.classList.add("in-window");
            }
            if (ackedSet.has(idx) || idx < base) {
                cell.classList.add("acked");
            }
        });
        // Auto scroll window tape to keep base visible
        const baseCell = document.getElementById(`pkt-cell-${base}`);
        if (baseCell) {
            baseCell.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "center" });
        }
    }

    // --- FSM State Badge ---
    function updateFsmState(stateName) {
        fsmStateName.textContent = stateName;
        fsmBadge.className = "fsm-badge";
        if (stateName === "STARTUP") {
            fsmBadge.classList.add("state-startup");
        } else if (stateName === "STABLE") {
            fsmBadge.classList.add("state-stable");
        } else if (stateName === "CONGESTION_DETECTED") {
            fsmBadge.classList.add("state-congestion");
        } else if (stateName === "RECOVERY") {
            fsmBadge.classList.add("state-recovery");
        } else {
            fsmBadge.classList.add("state-startup");
        }
    }

    // --- Event Logging ---
    function logEvent(type, message) {
        eventCount++;
        eventCountBadge.textContent = `${eventCount} events`;
        const entry = document.createElement("div");
        entry.className = `feed-entry ${type}`;
        const time = new Date().toLocaleTimeString().split(" ")[0];
        entry.textContent = `[${time}] ${message}`;
        eventsFeed.appendChild(entry);
        eventsFeed.scrollTop = eventsFeed.scrollHeight;
    }

    // --- Flight Token Animation ---
    function spawnFlightToken(seq, type = "data") {
        const token = document.createElement("div");
        token.className = `flight-token ${type}`;
        token.textContent = type === "data" ? `DATA #${seq}` : `ACK #${seq}`;
        packetRail.appendChild(token);
        setTimeout(() => {
            if (token.parentNode === packetRail) {
                packetRail.removeChild(token);
            }
        }, 1400);
    }

    // --- Launch Transfer ---
    launchBtn.addEventListener("click", async () => {
        if (state.isTransferring) return;
        state.isTransferring = true;
        launchBtn.disabled = true;
        launchBtn.querySelector(".btn-text").innerHTML = `<span class="icon">⏳</span> Transferring over UDP...`;

        // Reset display
        progressFill.style.width = "0%";
        progressPct.textContent = "0%";
        rateLabel.textContent = "0.0 KB/s";
        metricGoodput.textContent = "0.0";
        metricGoodputMbps.textContent = "0.000 Mbps";
        integrityBadge.className = "integrity-badge unverified";
        integrityBadge.innerHTML = "<span>⏳ Verification Pending...</span>";
        integrityStatus.textContent = "Transfer in Flight";

        initWindowTape();
        logEvent("system", `Initiating ${state.payloadKb} KB transfer via ${state.arq} (Mode: ${state.mode})...`);

        try {
            const response = await fetch("/api/transfer", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    arq_type: state.arq,
                    mode: state.mode,
                    payload_kb: state.payloadKb,
                    loss_rate: state.lossRate,
                    delay_ms: state.delayMs,
                    jitter_ms: state.jitterMs,
                    reorder_rate: state.reorderRate,
                    corrupt_rate: state.corruptRate,
                    dup_rate: state.dupRate,
                    seed: state.seed
                })
            });

            if (!response.ok) {
                throw new Error(`Server returned status ${response.status}`);
            }

            const result = await response.json();
            handleTransferComplete(result);

        } catch (err) {
            logEvent("timeout", `Transfer error: ${err.message}`);
            integrityStatus.textContent = "Transfer Failed";
        } finally {
            state.isTransferring = false;
            launchBtn.disabled = false;
            launchBtn.querySelector(".btn-text").innerHTML = `<span class="icon">🚀</span> Launch Reliable UDP Transfer`;
        }
    });

    function handleTransferComplete(data) {
        progressFill.style.width = "100%";
        progressPct.textContent = "100%";
        transferredBytesLabel.textContent = `${state.payloadKb} / ${state.payloadKb} KB`;

        metricGoodput.textContent = data.goodput_kb_s.toFixed(1);
        metricGoodputMbps.textContent = `${data.goodput_mbps.toFixed(3)} Mbps`;
        metricRtt.textContent = data.avg_rtt_ms.toFixed(1);
        metricRto.textContent = `Dynamic RTO: ${(data.current_rto_ms || 250).toFixed(0)} ms`;
        metricOverhead.textContent = data.overhead_pct.toFixed(1);
        metricRetransCount.textContent = `${data.retransmissions} retransmissions`;
        metricTimeouts.textContent = data.timeouts;

        srcShaLabel.textContent = data.source_sha256;
        dstShaLabel.textContent = data.received_sha256;

        if (data.integrity_ok) {
            integrityBadge.className = "integrity-badge verified";
            integrityBadge.innerHTML = "<span>🛡️ 100% Cryptographically Verified</span>";
            integrityStatus.textContent = "SHA-256 Hash Match Verified";
            logEvent("ack", `Transfer successful in ${data.elapsed_s.toFixed(3)}s! SHA-256 matched.`);
        } else {
            integrityBadge.className = "integrity-badge unverified";
            integrityBadge.innerHTML = "<span>❌ Hash Mismatch</span>";
            integrityStatus.textContent = "Corruption Detected!";
            logEvent("timeout", "File integrity validation failed!");
        }

        // Simulate animated flight tokens across rail
        for (let i = 0; i < 4; i++) {
            setTimeout(() => spawnFlightToken(i, "data"), i * 200);
            setTimeout(() => spawnFlightToken(i, "ack"), i * 200 + 300);
        }

        updateWindowTape(data.total_chunks || 30, state.mode === "dart" ? 8 : 4);
        updateFsmState(state.mode === "dart" ? "STABLE" : "STATIC_P3");
    }

    // --- Experiment Plot Inspection Modal ---
    document.querySelectorAll(".thumb-btn").forEach(btn => {
        btn.addEventListener("click", () => {
            const plotFile = btn.dataset.plot;
            modalPlotTitle.textContent = btn.title || "P3 Experiment Plot";
            modalPlotImg.src = `/api/plots/${plotFile}?t=${Date.now()}`;
            plotModal.style.display = "flex";
        });
    });

    modalCloseBtn.addEventListener("click", () => {
        plotModal.style.display = "none";
    });

    plotModal.addEventListener("click", (e) => {
        if (e.target === plotModal) {
            plotModal.style.display = "none";
        }
    });

    // Initial setup
    initWindowTape();
    updateFsmState("STARTUP");
});
