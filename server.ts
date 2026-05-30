import express from "express";
import path from "path";
import { createServer as createViteServer } from "vite";
import mqtt from "mqtt";
import { GoogleGenAI, Type } from "@google/genai";
import dotenv from "dotenv";
import { fileURLToPath } from "url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3000;

app.use(express.json());

// Initialize Gemini API
const ai = new GoogleGenAI({
  apiKey: process.env.GEMINI_API_KEY || "",
  httpOptions: {
    headers: {
      'User-Agent': 'aistudio-build',
    }
  }
});

// Broker configurations (matching ESP32 firmware)
const BROKERS = [
  { server: "kingfisher.lmq.cloudamqp.com",         port: 8883, user: "wxoeelnh", pass: "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x", clientId: "ESP32AMQP", vhost: "wxoeelnh", exactClientId: false },
  { server: "node02.myqtthub.com",                  port: 8883, user: "ESP",    pass: "a",                                 clientId: "WebClient",     vhost: null,       exactClientId: false },
  { server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud", port: 8883, user: "Web",    pass: "a",                                 clientId: "WebClient",    vhost: null,       exactClientId: false }
];

// App current live in-memory state
const systemState = {
  relays: [false, false, false, false], // [Relay1, Relay2, Relay3, Relay4]
  variasiMode: 0, // 0 = STOP, 1 = Maju, 2 = Mundur
  variasiJeda: 50, // default 50 ms
  activeBrokerIdx: 0,
  brokerConnected: false,
  temperature: 27.5,
  humidity: 65.0,
  lastUpdated: new Date().toISOString()
};

// Activity log tracking
interface ActivityEvent {
  id: string;
  time: string;
  type: string;
  detail: string;
  origin: string;
}

let activityEvents: ActivityEvent[] = [
  {
    id: `evt_${Date.now()}_init`,
    time: new Date().toISOString(),
    type: "system",
    detail: "Sistem Gateway MQTT Berhasil Diinisialisasi.",
    origin: "system"
  }
];

// Helper to push and trim log history
function addEvent(type: string, detail: string, origin: string) {
  const newEvt: ActivityEvent = {
    id: `evt_${Date.now()}_${Math.random().toString(36).substring(2, 5)}`,
    time: new Date().toISOString(),
    type,
    detail,
    origin
  };
  activityEvents.unshift(newEvt);
  if (activityEvents.length > 30) {
    activityEvents = activityEvents.slice(0, 30);
  }
}

// SSE Connection state
let sseClients: any[] = [];

function broadcastStateToClients() {
  const payload = JSON.stringify({
    type: "state_update",
    state: systemState,
    events: activityEvents
  });
  sseClients.forEach(c => {
    try {
      c.write(`data: ${payload}\n\n`);
    } catch (e) {
      // client disconnected
    }
  });
}

// Active MQTT connection reference
let mqttClient: mqtt.MqttClient | null = null;

// Support transactional publish for serverless (Vercel) environment
async function publishMqttServerless(brokerIdx: number, topic: string, payload: string): Promise<boolean> {
  const broker = BROKERS[brokerIdx];
  const loginUser = broker.vhost ? `${broker.vhost}:${broker.user}` : broker.user;
  const useExact = (broker as any).exactClientId || broker.clientId === "hebat-web-client" || broker.clientId === "WebClient" || broker.clientId === "ESP32AMQP";
  const uniqueClientId = useExact ? broker.clientId : `${broker.clientId}_vercel_${Math.random().toString(36).substring(2, 6)}`;
  
  const customPort = parseInt(broker.port as any) || 1883;
  const isCommonWsPort = [80, 443, 8000, 8080, 8083, 8084, 15675, 15676, 31443].includes(customPort);

  let connectUrl = "";
  if (isCommonWsPort) {
    const isSecure = [443, 8084, 15676, 31443].includes(customPort);
    const wsProto = isSecure ? "wss" : "ws";
    let path = "";
    if (broker.server.includes("cloudamqp.com")) {
      path = "/ws";
    } else if (broker.server.includes("myqtthub.com")) {
      path = "/mqtt";
    } else if (broker.server.includes("cedalo.cloud")) {
      path = "/mqtt";
    } else if (customPort === 15675 || customPort === 15676) {
      path = "/ws";
    } else if (customPort === 8083 || customPort === 8084) {
      path = "/mqtt";
    }
    connectUrl = `${wsProto}://${broker.server}:${customPort}${path}`;
  } else if (broker.server.includes("cloudamqp.com")) {
    connectUrl = `wss://${broker.server}:443/ws`;
  } else if (broker.server.includes("myqtthub.com")) {
    connectUrl = `wss://${broker.server}:443/mqtt`;
  } else if (broker.server.includes("cedalo.cloud")) {
    connectUrl = `wss://${broker.server}:443/mqtt`;
  } else {
    const protocol = customPort === 1883 || customPort === 1884 ? "mqtt" : "mqtts";
    connectUrl = `${protocol}://${broker.server}:${customPort}`;
  }

  return new Promise((resolve) => {
    console.log(`[Vercel MQTT] Connecting to publish on ${connectUrl} as ${uniqueClientId}...`);
    const client = mqtt.connect(connectUrl, {
      username: loginUser,
      password: broker.pass,
      clientId: uniqueClientId,
      rejectUnauthorized: false,
      connectTimeout: 5000,
    });

    let resolved = false;

    const cleanup = () => {
      if (!resolved) {
        resolved = true;
        try {
          client.end();
        } catch (e) {}
      }
    };

    client.on("connect", () => {
      console.log(`[Vercel MQTT] Connected! Publishing topic "${topic}" => "${payload}"`);
      client.publish(topic, payload, { qos: 1 }, (err) => {
        if (err) {
          console.error("[Vercel MQTT] Publish failed:", err);
        } else {
          console.log("[Vercel MQTT] Publish succeeded!");
        }
        cleanup();
        resolve(!err);
      });
    });

    client.on("error", (err) => {
      console.error("[Vercel MQTT] Connection error:", err.message);
      cleanup();
      resolve(false);
    });

    // Enforce 4-second cutoff
    setTimeout(() => {
      if (!resolved) {
        console.warn("[Vercel MQTT] Publish transaction timed out.");
        cleanup();
        resolve(false);
      }
    }, 4000);
  });
}

function connectMQTT(brokerIdx: number) {
  if (mqttClient) {
    try {
      mqttClient.end();
    } catch (e) {
      console.error("[MQTT] Error ending previous client:", e);
    }
  }

  const broker = BROKERS[brokerIdx];
  systemState.activeBrokerIdx = brokerIdx;
  systemState.brokerConnected = false;
  
  const loginUser = broker.vhost ? `${broker.vhost}:${broker.user}` : broker.user;
  const useExact = (broker as any).exactClientId || broker.clientId === "hebat-web-client" || broker.clientId === "WebClient" || broker.clientId === "ESP32AMQP";
  const uniqueClientId = useExact ? broker.clientId : `${broker.clientId}_web_${Math.random().toString(36).substring(2, 6)}`;
  
  const customPort = parseInt(broker.port as any) || 1883;
  const isCommonWsPort = [80, 443, 8000, 8080, 8083, 8084, 15675, 15676, 31443].includes(customPort);

  let connectUrl = "";
  if (isCommonWsPort) {
    const isSecure = [443, 8084, 15676, 31443].includes(customPort);
    const wsProto = isSecure ? "wss" : "ws";
    let path = "";
    if (broker.server.includes("cloudamqp.com")) {
      path = "/ws";
    } else if (broker.server.includes("myqtthub.com")) {
      path = "/mqtt";
    } else if (broker.server.includes("cedalo.cloud")) {
      path = "/mqtt";
    } else if (customPort === 15675 || customPort === 15676) {
      path = "/ws";
    } else if (customPort === 8083 || customPort === 8084) {
      path = "/mqtt";
    }
    connectUrl = `${wsProto}://${broker.server}:${customPort}${path}`;
  } else if (broker.server.includes("cloudamqp.com")) {
    connectUrl = `wss://${broker.server}:443/ws`;
  } else if (broker.server.includes("myqtthub.com")) {
    connectUrl = `wss://${broker.server}:443/mqtt`;
  } else if (broker.server.includes("cedalo.cloud")) {
    connectUrl = `wss://${broker.server}:443/mqtt`;
  } else {
    const isMqtts = customPort === 8883 || customPort === 8884 || customPort !== 1883;
    const protocol = isMqtts ? "mqtts" : "mqtt";
    connectUrl = `${protocol}://${broker.server}:${customPort}`;
  }

  console.log(`[MQTT] Connecting to Broker #${brokerIdx + 1} (${connectUrl}) as ${uniqueClientId}...`);
  addEvent("system", `Koneksi ke Broker ${brokerIdx + 1} (${broker.server}:${customPort}) dimulai...`, "system");
  broadcastStateToClients();

  try {
    mqttClient = mqtt.connect(connectUrl, {
      username: loginUser,
      password: broker.pass,
      clientId: uniqueClientId,
      rejectUnauthorized: false, // Mirip ESP32 wifiClient.setInsecure()
      connectTimeout: 8000,
      reconnectPeriod: 4000,
    });

    mqttClient.on("connect", () => {
      systemState.brokerConnected = true;
      console.log(`[MQTT] Connected to Broker #${brokerIdx + 1}`);
      addEvent("broker", `Berhasil tersambung ke Broker ${brokerIdx + 1} (${broker.server})`, "system");
      broadcastStateToClients();

      // Subscribe to all topics matching ESP32 firmware
      const topics = [
        "kontrol/relay1",
        "kontrol/relay2",
        "kontrol/relay3",
        "kontrol/relay4",
        "kontrol/variasi",
        "kontrol/variasi/jeda",
        "kontrol/broker",
        "status/broker",
        "sensor/suhu",
        "sensor/kelembaban"
      ];
      
      mqttClient?.subscribe(topics, (err) => {
        if (err) {
          console.error("[MQTT] Subscription failed:", err);
          addEvent("system", `Gagal melakukan subscribe topik: ${err.message}`, "system");
        } else {
          console.log("[MQTT] Subscribed to topics cleanly.");
        }
      });
    });

    mqttClient.on("message", (topic, message) => {
      const value = message.toString().trim();
      handleIncomingMqttMessage(topic, value);
    });

    mqttClient.on("error", (err) => {
      console.error(`[MQTT ERROR] Broker ${brokerIdx + 1}:`, err.message);
      systemState.brokerConnected = false;
      addEvent("system", `Broker ${brokerIdx + 1} error: ${err.message}`, "system");
      broadcastStateToClients();
    });

    mqttClient.on("close", () => {
      if (systemState.brokerConnected) {
        systemState.brokerConnected = false;
        console.log(`[MQTT] Disconnected from Broker ${brokerIdx + 1}`);
        addEvent("broker", `Koneksi ke Broker ${brokerIdx + 1} terputus`, "system");
        broadcastStateToClients();
      }
    });

  } catch (err: any) {
    console.error("[MQTT Exception]:", err);
    addEvent("system", `Exception ketika inisialisasi broker: ${err.message}`, "system");
    broadcastStateToClients();
  }
}

function handleIncomingMqttMessage(topic: string, value: string) {
  let changed = false;

  if (topic === "sensor/suhu") {
    const t = parseFloat(value);
    if (!isNaN(t)) {
      systemState.temperature = parseFloat(t.toFixed(1));
      addEvent("sensor", `Suhu termonitor: ${systemState.temperature}°C`, "esp32");
      changed = true;
    }
  } else if (topic === "sensor/kelembaban") {
    const h = parseFloat(value);
    if (!isNaN(h)) {
      systemState.humidity = parseFloat(h.toFixed(1));
      addEvent("sensor", `Kelembaban termonitor: ${systemState.humidity}%`, "esp32");
      changed = true;
    }
  } else if (topic === "status/broker") {
    // Format "BROKER:X|server_address"
    addEvent("broker", `ESP32 status: ${value}`, "esp32");
    const parts = value.split("|");
    if (parts[0] && parts[0].startsWith("BROKER:")) {
      const idx = parseInt(parts[0].substring(7)) - 1;
      if (idx >= 0 && idx <= 2 && idx !== systemState.activeBrokerIdx) {
        console.log(`[Sync] ESP32 has fallen back or requested Broker #${idx + 1}. Aligning server...`);
        addEvent("broker", `Aplikasi menyelaraskan server dengan ESP32 ke Broker ${idx + 1}`, "system");
        changed = true;
        setTimeout(() => connectMQTT(idx), 200);
      }
    }
  } else if (topic === "kontrol/relay1") {
    const nextVal = (value === "ON");
    if (systemState.relays[0] !== nextVal) {
      systemState.relays[0] = nextVal;
      addEvent("relay", `Relay 1 diatur: ${value}`, "esp32");
      changed = true;
    }
  } else if (topic === "kontrol/relay2") {
    const nextVal = (value === "ON");
    if (systemState.relays[1] !== nextVal) {
      systemState.relays[1] = nextVal;
      addEvent("relay", `Relay 2 diatur: ${value}`, "esp32");
      changed = true;
    }
  } else if (topic === "kontrol/relay3") {
    const nextVal = (value === "ON");
    if (systemState.relays[2] !== nextVal) {
      systemState.relays[2] = nextVal;
      addEvent("relay", `Relay 3 diatur: ${value}`, "esp32");
      changed = true;
    }
  } else if (topic === "kontrol/relay4") {
    const nextVal = (value === "ON");
    if (systemState.relays[3] !== nextVal) {
      systemState.relays[3] = nextVal;
      addEvent("relay", `Relay 4 diatur: ${value}`, "esp32");
      changed = true;
    }
  } else if (topic === "kontrol/variasi") {
    if (value === "STOP") {
      systemState.variasiMode = 0;
      addEvent("variasi", `Animasi variasi dihentikan`, "esp32");
    } else {
      const mode = parseInt(value);
      if (mode === 1 || mode === 2) {
        systemState.variasiMode = mode;
        addEvent("variasi", `Variasi mode ${mode} diaktifkan`, "esp32");
      }
    }
    changed = true;
  } else if (topic === "kontrol/variasi/jeda") {
    const jeda = parseInt(value);
    if (!isNaN(jeda) && jeda >= 50 && jeda <= 500) {
      systemState.variasiJeda = jeda;
      addEvent("variasi", `Jeda bervariasi diperbaharui ke ${jeda} ms`, "esp32");
      changed = true;
    }
  } else if (topic === "kontrol/broker") {
    const idx = parseInt(value) - 1;
    if (idx >= 0 && idx <= 2 && idx !== systemState.activeBrokerIdx) {
      addEvent("broker", `Permintaan ganti broker terdeteksi ke Broker ${idx + 1}`, "esp32");
      systemState.activeBrokerIdx = idx;
      setTimeout(() => connectMQTT(idx), 200);
      changed = true;
    }
  }

  if (changed) {
    systemState.lastUpdated = new Date().toISOString();
    broadcastStateToClients();
  }
}

// -------------------------------------------------------------
//  REST API ROUTING
// -------------------------------------------------------------

// 1. Stream Endpoint (SSE)
app.get("/api/stream", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("Access-Control-Allow-Origin", "*");

  // Send baseline package
  const baseline = JSON.stringify({
    type: "init",
    state: systemState,
    events: activityEvents,
    brokers: BROKERS.map((b, i) => ({ 
      id: i + 1, 
      server: b.server, 
      port: b.port,
      user: b.user,
      pass: b.pass,
      clientId: b.clientId,
      vhost: b.vhost
    }))
  });
  res.write(`data: ${baseline}\n\n`);

  sseClients.push(res);

  req.on("close", () => {
    sseClients = sseClients.filter(c => c !== res);
  });
});

// 2. Fetch baseline state
app.get("/api/state", (req, res) => {
  res.json({
    state: systemState,
    events: activityEvents,
    brokers: BROKERS.map((b, i) => ({ 
      id: i + 1, 
      server: b.server, 
      port: b.port,
      user: b.user,
      pass: b.pass,
      clientId: b.clientId,
      vhost: b.vhost
    }))
  });
});

// 2b. Update broker custom configurations dynamically
app.post("/api/update-broker", (req, res) => {
  const { index, server, port, user, pass, clientId, vhost } = req.body;
  const idx = parseInt(index);
  if (isNaN(idx) || idx < 0 || idx > 2) {
    res.status(400).json({ error: "Indeks broker tidak valid (0 - 2)." });
    return;
  }

  if (!server) {
    res.status(400).json({ error: "Hostname server wajib diisi." });
    return;
  }

  BROKERS[idx] = {
    server,
    port: parseInt(port) || 1883,
    user: user || "",
    pass: pass || "",
    clientId: clientId || `ESP32_${Math.random().toString(36).substring(2, 6).toUpperCase()}`,
    vhost: vhost || null,
    exactClientId: clientId === "hebat-web-client" || clientId === "WebClient"
  } as any;

  addEvent("broker", `Konfigurasi Broker ${idx + 1} diperbarui ke: ${server}:${port}`, "web");

  // Reconnect automatically if we are currently using this broker
  if (systemState.activeBrokerIdx === idx) {
    console.log(`[MQTT] Reconnecting to updated Broker #${idx + 1}...`);
    connectMQTT(idx);
  } else {
    broadcastStateToClients();
  }

  res.json({ 
    success: true, 
    brokers: BROKERS.map((b, i) => ({ 
      id: i + 1, 
      server: b.server, 
      port: b.port,
      user: b.user,
      pass: b.pass,
      clientId: b.clientId,
      vhost: b.vhost
    }))
  });
});

// 2c. Sync all broker configurations from client localStorage (e.g. on start)
app.post("/api/sync-brokers", (req, res) => {
  const { brokers } = req.body;
  if (Array.isArray(brokers) && brokers.length === 3) {
    brokers.forEach((b: any, idx: number) => {
      if (b && b.server) {
        BROKERS[idx] = {
          server: b.server,
          port: parseInt(b.port) || 1883,
          user: b.user || "",
          pass: b.pass || "",
          clientId: b.clientId || `ESP32_${Math.random().toString(36).substring(2, 6).toUpperCase()}`,
          vhost: b.vhost || null,
          exactClientId: b.clientId === "hebat-web-client" || b.clientId === "WebClient" || b.clientId === "ESP32AMQP"
        } as any;
      }
    });
    console.log("[Sync] Synchronized broker configurations from browser client storage.");
    
    // Trigger reconnection if needed to the now-synced configurations of the active broker idx
    const activeIdx = systemState.activeBrokerIdx;
    if (activeIdx >= 0 && activeIdx <= 2) {
      console.log(`[Sync] Re-connecting with newly synced credentials to Broker #${activeIdx + 1}`);
      connectMQTT(activeIdx);
    }
    
    res.json({ success: true, message: "Status disinkronkan" });
  } else {
    res.status(400).json({ error: "Data sync tidak valid." });
  }
});

// 3. Publish Control MQTT Topic
app.post("/api/control", async (req, res) => {
  const { topic, payload } = req.body;
  if (!topic || payload === undefined || payload === null) {
    res.status(400).json({ error: "Parameter topic dan payload wajib diisi." });
    return;
  }

  const payloadStr = String(payload).trim();
  console.log(`[REST Publish] Topic: ${topic} => Payload: ${payloadStr}`);

  // Push event local
  addEvent(topic.split("/")[1] || "system", `Instruksi Web: ${topic} => ${payloadStr}`, "web");

  if (process.env.VERCEL) {
    const success = await publishMqttServerless(systemState.activeBrokerIdx, topic, payloadStr);
    console.log(`[Vercel REST Publish] Transactional publish output success=${success}`);
  } else if (mqttClient && systemState.brokerConnected) {
    mqttClient.publish(topic, payloadStr, { qos: 1 });
  } else {
    // If and only if broker is disconnected, we automatically self-apply this
    // so user can preview/simulate offline operations smoothly inside the editor
    setTimeout(() => {
      handleIncomingMqttMessage(topic, payloadStr);
    }, 50);
  }

  // Self apply locally immediately for snappy responsiveness
  handleIncomingMqttMessage(topic, payloadStr);

  res.json({ status: "queued", topic, payload: payloadStr });
});

// 4. Manual Broker Change Trigger
app.post("/api/switch-broker", async (req, res) => {
  const { index } = req.body;
  const idx = parseInt(index);
  if (isNaN(idx) || idx < 0 || idx > 2) {
    res.status(400).json({ error: "Indeks broker tidak valid (0 - 2)." });
    return;
  }

  addEvent("broker", `Permintaan ganti broker ke: Broker ${idx + 1}`, "web");

  if (process.env.VERCEL) {
    await publishMqttServerless(systemState.activeBrokerIdx, "kontrol/broker", `${idx + 1}`);
    systemState.activeBrokerIdx = idx;
  } else {
    // Publish to the current MQTT broker so ESP32 knows to switch broker
    if (mqttClient && systemState.brokerConnected) {
      console.log(`[MQTT Switch] Sending ganti broker perintah to topic kontrol/broker: ${idx + 1}`);
      mqttClient.publish("kontrol/broker", `${idx + 1}`, { qos: 1 });
    }

    // Perform broker switch locally on the server
    setTimeout(() => {
      connectMQTT(idx);
    }, 300);
  }

  res.json({ status: "switching", target_broker_index: idx });
});

// 5. Intelligent voice-command processor using Gemini 3.5 Flash
app.post("/api/voice-command", async (req, res) => {
  const { transcript } = req.body;
  if (!transcript || typeof transcript !== "string") {
    res.status(400).json({ error: "Parameter transcript (string) wajib disertakan." });
    return;
  }

  console.log(`[AI Voice Command] Processing transcript: "${transcript}"`);
  addEvent("voice", `Analisis perintah suara: "${transcript}"`, "web");

  try {
    const systemPrompt = `You are an AI Smart Home and IoT Gateway assistant. Your task is to analyze Indonesian voice instructions for a 4-channel ESP32 Relay board with Fallback Brokers and Variasi Modes.

Translate the user's spoken command into a structured list of MQTT publish actions.

Supported Topics and Payloads:
1. "kontrol/relay1" => "ON" or "OFF"
2. "kontrol/relay2" => "ON" or "OFF"
3. "kontrol/relay3" => "ON" or "OFF"
4. "kontrol/relay4" => "ON" or "OFF"
5. "kontrol/variasi" => "1" (Maju), "2" (Mundur), or "STOP"
6. "kontrol/variasi/jeda" => An integer string between '50' and '500' representing milliseconds (e.g. "120")
7. "kontrol/broker" => "1", "2", or "3" (Active Broker Switch)

User speech examples & interpretation:
- "nyalakan lampu satu" => Publish "kontrol/relay1" = "ON"
- "matikan semua relay" => Publish "kontrol/relay1" = "OFF", "kontrol/relay2" = "OFF", "kontrol/relay3" = "OFF", "kontrol/relay4" = "OFF"
- "matikan semua" => Publish all relays to OFF and "kontrol/variasi" to "STOP"
- "nyalakan semua" => Publish all relays to ON and "kontrol/variasi" to "STOP"
- "aktifkan animasi maju relay" or "bikin variasi berurutan maju" => "kontrol/variasi" = "1"
- "variasi mundur" or "relay sirkuit mundur" => "kontrol/variasi" = "2"
- "stop variasi" or "hentikan variasi" => "kontrol/variasi" = "STOP"
- "ganti server ke tiga" or "ganti broker satu" => "kontrol/broker" = "1" (or correct broker index)
- "atur jeda bervariasi jadi 150 ms" or "bikin cepat jeda 100" => "kontrol/variasi/jeda" = "100" (extract millisecond value, clamp between 50 and 500)

Understand synonyms like "buka", "hidupkan", "aktifkan", "hubungkan", "start", "on" for ON, and "tutup", "mati", "nonaktifkan", "stop", "off" for OFF.
Relay indices are always "1", "2", "3", "4" (translated from "satu", "dua", "tiga", "empat").

Generate a JSON object conforming exactly to the following properties:
- 'commands': Array of targets, each with 'topic' (string) and 'payload' (string).
- 'message': A helpful local feedback text in Indonesian explaining what the AI interpreted and decided to do. E.g. "Baik, mematikan semua relay dan menghentikan animasi."`;

    const response = await ai.models.generateContent({
      model: "gemini-3.5-flash",
      contents: transcript,
      config: {
        systemInstruction: systemPrompt,
        temperature: 0.1,
        responseMimeType: "application/json",
        responseSchema: {
          type: Type.OBJECT,
          properties: {
            commands: {
              type: Type.ARRAY,
              items: {
                type: Type.OBJECT,
                properties: {
                  topic: { type: Type.STRING },
                  payload: { type: Type.STRING }
                },
                required: ["topic", "payload"]
              }
            },
            message: { type: Type.STRING }
          },
          required: ["commands", "message"]
        }
      }
    });

    const resultText = response.text || "{}";
    const data = JSON.parse(resultText);

    console.log(`[AI Voice Parsed] Response:`, data);

    if (data.commands && Array.isArray(data.commands)) {
      for (const cmd of data.commands) {
        if (cmd.topic && cmd.payload !== undefined) {
          addEvent("voice", `Aksi Otomatis Voice: ${cmd.topic} => ${cmd.payload}`, "system");
          
          if (cmd.topic === "kontrol/broker") {
            // Broker index change
            const bIdx = parseInt(cmd.payload) - 1;
            if (bIdx >= 0 && bIdx <= 2) {
              if (process.env.VERCEL) {
                await publishMqttServerless(systemState.activeBrokerIdx, "kontrol/broker", `${bIdx + 1}`);
                systemState.activeBrokerIdx = bIdx;
              } else {
                if (mqttClient && systemState.brokerConnected) {
                  mqttClient.publish("kontrol/broker", `${bIdx + 1}`, { qos: 1 });
                }
                setTimeout(() => connectMQTT(bIdx), 300);
              }
            }
          } else {
            // Normal system topic publish
            if (process.env.VERCEL) {
              await publishMqttServerless(systemState.activeBrokerIdx, cmd.topic, String(cmd.payload));
            } else if (mqttClient && systemState.brokerConnected) {
              mqttClient.publish(cmd.topic, String(cmd.payload), { qos: 1 });
            }
            handleIncomingMqttMessage(cmd.topic, String(cmd.payload));
          }
        }
      }
      systemState.lastUpdated = new Date().toISOString();
      broadcastStateToClients();
    }

    res.json({
      success: true,
      message: data.message || "Perintah berhasil diproses.",
      commands: data.commands || []
    });

  } catch (err: any) {
    console.error("[AI Voice Error]:", err);
    res.status(500).json({ error: "Gemini AI gagal memproses intonasi perintah suara.", detail: err.message });
  }
});

// 6. Simulator mode to drift values when ESP32 is offline
let simulatorInterval: NodeJS.Timeout | null = null;
app.post("/api/simulator", (req, res) => {
  const { enabled } = req.body;
  if (enabled) {
    if (!simulatorInterval) {
      console.log("[SIMULATOR] Starting ESP32 telemetry simulator drift...");
      addEvent("system", "Mode Simulator Telemetri ESP32 diaktifkan.", "system");
      
      simulatorInterval = setInterval(() => {
        // Drift temperature slightly
        const tempDelta = (Math.random() - 0.5) * 0.4;
        systemState.temperature = parseFloat(Math.min(35, Math.max(16, systemState.temperature + tempDelta)).toFixed(1));

        // Drift humidity slightly
        const humDelta = (Math.random() - 0.5) * 1.0;
        systemState.humidity = parseFloat(Math.min(95, Math.max(40, systemState.humidity + humDelta)).toFixed(1));

        // If variasi mode is active, simulate step change
        if (systemState.variasiMode > 0) {
          // just trigger a visual log to make the dashboard lively!
          const steps = ["Maju", "Mundur"];
          console.log(`[SIMULATOR] Animasi relay bervariasi (${steps[systemState.variasiMode-1]}) aktif.`);
        }

        systemState.lastUpdated = new Date().toISOString();
        broadcastStateToClients();
      }, 5000);
    }
  } else {
    if (simulatorInterval) {
      clearInterval(simulatorInterval);
      simulatorInterval = null;
      console.log("[SIMULATOR] Telemetry simulator stopped.");
      addEvent("system", "Mode Simulator Telemetri ESP32 dinonaktifkan.", "system");
      broadcastStateToClients();
    }
  }
  res.json({ status: "ok", simulator_active: !!simulatorInterval });
});

// Set up frontend asset delivery
async function startViteProxy() {
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
    console.log("[Nginx/Vite] Development compilation middleware enabled.");
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
    console.log("[Production] Static files rendering mounted successfully.");
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`[SYSTEM RUNNING] Application is up and bound to host 0.0.0.0 on port ${PORT}`);
  });
}

// Only start direct MQTT listener loops and Express web server if NOT running on Vercel
if (!process.env.VERCEL) {
  // Start MQTT initial connection to default broker
  connectMQTT(0);
  startViteProxy();
}

export default app;
