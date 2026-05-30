import React, { useState, useEffect, useRef } from "react";
import mqtt from "mqtt";
import { 
  Power, 
  Thermometer, 
  Droplets, 
  Activity, 
  Database, 
  Server, 
  Sliders, 
  Volume2, 
  Play, 
  Square, 
  RotateCw, 
  Terminal, 
  Wifi, 
  WifiOff, 
  CheckCircle2, 
  AlertCircle, 
  Trash2, 
  Copy, 
  Cpu, 
  Send,
  Mic,
  MicOff,
  Sparkles,
  RefreshCw,
  Clock,
  Check,
  Settings
} from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { SystemState, ActivityEvent } from "./types";

export default function App() {
  // State from server/MQTT
  const [state, setState] = useState<SystemState>({
    relays: [false, false, false, false],
    variasiMode: 0,
    variasiJeda: 50,
    activeBrokerIdx: 0,
    brokerConnected: false,
    temperature: 27.5,
    humidity: 64.2,
    lastUpdated: new Date().toISOString()
  });

  const [events, setEvents] = useState<ActivityEvent[]>([]);
  const [brokers, setBrokers] = useState<any[]>(() => {
    const defaultBrokers = [
      { id: 1, name: "CloudAMQP (Primary)",         server: "kingfisher.lmq.cloudamqp.com",         port: 8883, user: "wxoeelnh", pass: "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x", clientId: "ESP32AMQP", vhost: "wxoeelnh" },
      { id: 2, name: "MyQtthub (Backup)",           port: 8883, server: "node02.myqtthub.com",                  user: "ESP",    pass: "a",                                 clientId: "WebClient",     vhost: null },
      { id: 3, name: "Cedalo Cloud (Fallback)",     port: 8883, server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud", user: "Web",    pass: "a",                                 clientId: "WebClient",    vhost: null }
    ];
    const cached = localStorage.getItem("mqtt_brokers");
    if (cached) {
      try {
        const parsed = JSON.parse(cached);
        if (Array.isArray(parsed) && parsed.length > 0) {
          // Auto upgrade node02.myqtthub.com port 1883 with 8883
          return parsed.map((item: any) => {
            if (item && item.server === "node02.myqtthub.com" && item.port === 1883) {
              return { ...item, port: 8883 };
            }
            return item;
          });
        }
      } catch (e) {
        console.error("Failed parsing initial cached brokers:", e);
      }
    }
    return defaultBrokers;
  });
  const [isSimulatorActive, setIsSimulatorActive] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<"CONNECTED" | "CONNECTING" | "DISCONNECTED">("CONNECTING");

  // Broker configuration edit states
  const [editingBrokerIdx, setEditingBrokerIdx] = useState<number | null>(null);
  const [editServer, setEditServer] = useState("");
  const [editPort, setEditPort] = useState("8883");
  const [editUser, setEditUser] = useState("");
  const [editPass, setEditPass] = useState("");
  const [editClientId, setEditClientId] = useState("ESP32AMQP");
  const [editVhost, setEditVhost] = useState("");

  // Voice Command & Speech states
  const [isRecording, setIsRecording] = useState(false);
  const [transcript, setTranscript] = useState("");
  const [textCommand, setTextCommand] = useState("");
  const [isAiProcessing, setIsAiProcessing] = useState(false);
  const [aiResponse, setAiResponse] = useState<string | null>(null);
  const [lastExecutedVoiceRules, setLastExecutedVoiceRules] = useState<any[]>([]);
  const [speechSupported, setSpeechSupported] = useState(false);
  const [copiedEventId, setCopiedEventId] = useState<string | null>(null);

  // References
  const recognitionRef = useRef<any>(null);
  const logContainerRef = useRef<HTMLDivElement>(null);
  const clientRef = useRef<any>(null);

  // Constants mapping
  const RELAY_LOADOUT_NAMES = [
    { title: "Relay 1: Kipas Angin", desc: "Cooling system" },
    { title: "Relay 2: Pompa Air", desc: "Watering controller" },
    { title: "Relay 3: Heater Sasis", desc: "Heating regulator" },
    { title: "Relay 4: Alarm Sirene", desc: "Emergency horn" }
  ];

  const BROKER_LIST = [
    { name: "CloudAMQP Premium TLS", server: "kingfisher.lmq.cloudamqp.com" },
    { name: "MyQttHub TLS", server: "node02.myqtthub.com" },
    { name: "Cedalo Cloud TLS", server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud" }
  ];

  // -------------------------------------------------------------
  //  SERVER SYNC & REALTIME PIPELINE
  // -------------------------------------------------------------
  
  useEffect(() => {
    // 1. Check Speech Recognition Support
    const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
    if (SpeechRecognition) {
      setSpeechSupported(true);
      const rec = new SpeechRecognition();
      rec.continuous = false;
      rec.interimResults = false;
      rec.lang = "id-ID"; // set language preference to Indonesian

      rec.onstart = () => {
        setIsRecording(true);
        setAiResponse(null);
        setTranscript("");
      };

      rec.onerror = (e: any) => {
        console.error("Speech recognition error:", e);
        setIsRecording(false);
      };

      rec.onend = () => {
        setIsRecording(false);
      };

      rec.onresult = (e: any) => {
        const resultText = e.results[0][0].transcript;
        setTranscript(resultText);
        sendVoiceCommand(resultText);
      };

      recognitionRef.current = rec;
    }

    // 2. Initial baseline load
    fetchBaselineState();

    // 3. Connect to EventStream (SSE)
    console.log("[Web] Setting up Server-Sent Events stream...");
    let sse = new EventSource("/api/stream");

    sse.onopen = () => {
      console.log("[Web] SSE stream connection established.");
      setConnectionStatus("CONNECTED");
    };

    sse.onerror = (err) => {
      console.error("[Web] SSE disconnected, falling back to polling mode...", err);
      setConnectionStatus("DISCONNECTED");
      sse.close();
    };

    sse.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === "init" || payload.type === "state_update") {
          setState(payload.state);
          setEvents(payload.events);
          if (payload.brokers) {
            let loadedBrokers = payload.brokers;
            // Normalize node02.myqtthub.com port 1883 to 8883
            loadedBrokers = loadedBrokers.map((b: any) => {
              if (b && b.server === "node02.myqtthub.com" && b.port === 1883) {
                return { ...b, port: 8883 };
              }
              return b;
            });
            const cached = localStorage.getItem("mqtt_brokers");
            if (cached) {
              try {
                const parsed = JSON.parse(cached);
                if (Array.isArray(parsed) && parsed.length > 0) {
                  const defaultBrokers = [
                    { id: 1, name: "CloudAMQP (Primary)",         server: "kingfisher.lmq.cloudamqp.com",         port: 8883, user: "wxoeelnh", pass: "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x", clientId: "ESP32AMQP", vhost: "wxoeelnh" },
                    { id: 2, name: "MyQtthub (Backup)",           port: 8883, server: "node02.myqtthub.com",                  user: "ESP",    pass: "a",                                 clientId: "WebClient",     vhost: null },
                    { id: 3, name: "Cedalo Cloud (Fallback)",     port: 8883, server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud", user: "Web",    pass: "a",                                 clientId: "WebClient",    vhost: null }
                  ];
                  const clean = [];
                  for (let i = 0; i < 3; i++) {
                    const item = parsed[i];
                    if (item && typeof item === 'object' && item.server) {
                      const merged = { ...defaultBrokers[i], ...item };
                      if (merged.server === "node02.myqtthub.com" && merged.port === 1883) {
                        merged.port = 8883;
                      }
                      clean.push(merged);
                    } else {
                      clean.push(defaultBrokers[i]);
                    }
                  }
                  loadedBrokers = clean;
                }
              } catch (e) {
                console.error("Failed parsing cached brokers in SSE:", e);
              }
            } else {
              localStorage.setItem("mqtt_brokers", JSON.stringify(loadedBrokers));
            }
            setBrokers(loadedBrokers);
          }
          setConnectionStatus("CONNECTED");
        }
      } catch (err) {
        console.error("[Web] Error parsing event from stream:", err);
      }
    };

    // 4. Polling backup in case SSE is blocked by proxies/sandbox environment
    const interval = setInterval(() => {
      if (sse.readyState === EventSource.CLOSED) {
        setConnectionStatus("CONNECTING");
        fetchBaselineState();
        // Try to recreate SSE
        sse = new EventSource("/api/stream");
      }
    }, 4000);

    return () => {
      sse.close();
      clearInterval(interval);
    };
  }, []);

  // Auto-scroll log terminal internally to bottom on new actions
  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [events]);

  const addClientEvent = (
    type: "system" | "sensor" | "broker" | "relay" | "variasi" | "voice",
    detail: string,
    origin: "system" | "esp32" | "web"
  ) => {
    const newEvt: ActivityEvent = {
      id: `evt_cli_${Date.now()}_${Math.random().toString(36).substring(2, 5)}`,
      time: new Date().toISOString(),
      type,
      detail,
      origin
    };
    setEvents(prev => {
      const updated = [newEvt, ...prev];
      return updated.slice(0, 30);
    });
  };

  const getBrokerWsUrlAndOptions = (broker: any) => {
    let protocol = "wss";
    let wsPort = 443;
    let path = "";
    
    const server = broker.server || "";
    const user = broker.user || "";
    const pass = broker.pass || "";
    const vhost = broker.vhost || "";
    
    // Determine Port & Path dynamically
    const isHttps = window.location.protocol === "https:";
    protocol = isHttps ? "wss" : "ws";
    
    // Check if the user entered a custom port that looks like a WebSocket port
    const customPort = parseInt(broker.port);
    const isCommonWsPort = [80, 443, 8000, 8080, 8083, 8084, 15675, 15676, 31443].includes(customPort);

    if (server.includes("cloudamqp.com")) {
      // CloudAMQP LavinMQ/RabbitMQ
      // Standard WSS ports: 15676 (default management-plugin wss) or 443 (HTTPS WebSockets proxy)
      wsPort = isCommonWsPort ? customPort : 443;
      path = "/ws"; // LavinMQ expects '/ws' path for websockets
    } else if (server.includes("myqtthub.com")) {
      // MyQttHub
      // Standard WSS ports: 443 (HTTPS proxy WSS) or 8084 (Alternative WSS)
      wsPort = isCommonWsPort ? customPort : 443;
      path = "/mqtt"; // MyQttHub expects '/mqtt' path for WebSockets
    } else if (server.includes("cedalo.cloud")) {
      // Cedalo Cloud
      // Standard WSS port is 443
      wsPort = isCommonWsPort ? customPort : 443;
      path = "/mqtt";
    } else {
      // Other brokers
      wsPort = isHttps ? 443 : 1883;
      if (isCommonWsPort) {
        wsPort = customPort;
      }
      
      // Attempt smart paths depending on common ports
      if (wsPort === 15676 || wsPort === 15675) {
        path = "/ws";
      } else if (wsPort === 8083 || wsPort === 8084) {
        path = "/mqtt";
      }
    }
    
    const loginUser = vhost ? `${vhost}:${user}` : user;
    
    // Generate unique client name to prevent collision
    const useExact = broker.clientId === "hebat-web-client";
    const clientId = useExact ? broker.clientId : `${broker.clientId}_browser_${Math.random().toString(36).substring(2, 6)}`;
    
    let wsUrl = `${protocol}://${server}:${wsPort}`;
    if (path) {
      wsUrl += path;
    }
    
    return {
      url: wsUrl,
      options: {
        username: loginUser,
        password: pass,
        clientId: clientId,
        rejectUnauthorized: false,
        connectTimeout: 8000,
        reconnectPeriod: 4000,
      }
    };
  };

  const fetchBaselineState = async () => {
    try {
      const res = await fetch("/api/state");
      if (res.ok) {
        const data = await res.json();
        
        // Cache and merge brokers list
        let loadedBrokers = data.brokers || [];
        // Force upgrade node02.myqtthub.com port 1883 to 8883 to bypass firewalls
        loadedBrokers = loadedBrokers.map((b: any) => {
          if (b && b.server === "node02.myqtthub.com" && b.port === 1883) {
            return { ...b, port: 8883 };
          }
          return b;
        });
        const cached = localStorage.getItem("mqtt_brokers");
        if (cached) {
          try {
            const parsed = JSON.parse(cached);
            if (Array.isArray(parsed) && parsed.length > 0) {
              const defaultBrokers = [
                { id: 1, name: "CloudAMQP (Primary)",         server: "kingfisher.lmq.cloudamqp.com",         port: 8883, user: "wxoeelnh", pass: "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x", clientId: "ESP32AMQP", vhost: "wxoeelnh" },
                { id: 2, name: "MyQtthub (Backup)",           port: 8883, server: "node02.myqtthub.com",                  user: "ESP",    pass: "a",                                 clientId: "WebClient",     vhost: null },
                { id: 3, name: "Cedalo Cloud (Fallback)",     port: 8883, server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud", user: "Web",    pass: "a",                                 clientId: "WebClient",    vhost: null }
              ];
              const clean = [];
              for (let i = 0; i < 3; i++) {
                const item = parsed[i];
                if (item && typeof item === 'object' && item.server) {
                  const merged = { ...defaultBrokers[i], ...item };
                  if (merged.server === "node02.myqtthub.com" && merged.port === 1883) {
                    merged.port = 8883;
                  }
                  clean.push(merged);
                } else {
                  clean.push(defaultBrokers[i]);
                }
              }
              loadedBrokers = clean;
            }
          } catch (e) {
            console.error("Failed parsing cached brokers:", e);
          }
        } else {
          localStorage.setItem("mqtt_brokers", JSON.stringify(loadedBrokers));
        }
        
        setBrokers(loadedBrokers);
        
        setState(prev => ({
          ...prev,
          relays: data.state.relays,
          variasiMode: data.state.variasiMode,
          variasiJeda: data.state.variasiJeda,
          activeBrokerIdx: data.state.activeBrokerIdx,
          temperature: data.state.temperature,
          humidity: data.state.humidity,
          lastUpdated: data.state.lastUpdated
        }));
        
        setEvents(data.events || []);
        setConnectionStatus("CONNECTED");
      }
    } catch (e) {
      console.error("Error fetching state:", e);
      setConnectionStatus("DISCONNECTED");
      
      // Local backup fallback
      const cached = localStorage.getItem("mqtt_brokers");
      if (cached) {
        try {
          const parsed = JSON.parse(cached);
          if (Array.isArray(parsed) && parsed.length > 0) {
            const defaultBrokers = [
              { id: 1, name: "CloudAMQP (Primary)",         server: "kingfisher.lmq.cloudamqp.com",         port: 8883, user: "wxoeelnh", pass: "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x", clientId: "ESP32AMQP", vhost: "wxoeelnh" },
              { id: 2, name: "MyQtthub (Backup)",           port: 8883, server: "node02.myqtthub.com",                  user: "ESP",    pass: "a",                                 clientId: "WebClient",     vhost: null },
              { id: 3, name: "Cedalo Cloud (Fallback)",     port: 8883, server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud", user: "Web",    pass: "a",                                 clientId: "WebClient",    vhost: null }
            ];
            const clean = [];
            for (let i = 0; i < 3; i++) {
              const item = parsed[i];
              if (item && typeof item === 'object' && item.server) {
                const merged = { ...defaultBrokers[i], ...item };
                if (merged.server === "node02.myqtthub.com" && merged.port === 1883) {
                  merged.port = 8883;
                }
                clean.push(merged);
              } else {
                clean.push(defaultBrokers[i]);
              }
            }
            setBrokers(clean);
          }
        } catch (err) {}
      } else {
        setBrokers([
          { server: "kingfisher.lmq.cloudamqp.com",         port: 8883, user: "wxoeelnh", pass: "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x", clientId: "ESP32AMQP", vhost: "wxoeelnh" },
          { server: "node02.myqtthub.com",                  port: 8883, user: "ESP",    pass: "a",                                 clientId: "WebClient",     vhost: null },
          { server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud", port: 8883, user: "Web",    pass: "a",                                 clientId: "WebClient",    vhost: null }
        ]);
      }
    }
  };

  // -------------------------------------------------------------
  //  BROWSER DIRECT MQTT OVER WEBSOCKETS CLIENT ENGINE
  // -------------------------------------------------------------
  useEffect(() => {
    if (brokers.length === 0) return;
    
    const activeBroker = brokers[state.activeBrokerIdx];
    if (!activeBroker) return;
    
    // Close any prior MQTT connections
    if (clientRef.current) {
      try {
        console.log("[Browser MQTT] Closing previous connection...");
        clientRef.current.end();
      } catch (e) {
        console.error("[Browser MQTT] Error closing connection:", e);
      }
    }
    
    const connInfo = getBrokerWsUrlAndOptions(activeBroker);
    console.log(`[Browser MQTT] Connecting directly via WebSockets to: ${connInfo.url}`);
    
    addClientEvent("system", `Koneksi browser ke Broker ${state.activeBrokerIdx + 1} (${activeBroker.server}) dimulai...`, "system");
    
    setState(prev => ({ ...prev, brokerConnected: false }));
    
    try {
      const client = mqtt.connect(connInfo.url, connInfo.options);
      clientRef.current = client;
      
      client.on("connect", () => {
        console.log("[Browser MQTT] Connected to Brokered WebSocket!");
        setState(prev => ({ ...prev, brokerConnected: true }));
        addClientEvent("broker", `Browser sukses terhubung langsung ke Broker ${state.activeBrokerIdx + 1} (${activeBroker.server}) via WebSockets!`, "system");
        
        // Subscribe to exactly the same ESP32 topics
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
        
        client.subscribe(topics, (err) => {
          if (err) {
            console.error("[Browser MQTT] Subscribe fail:", err);
            addClientEvent("system", `Gagal melakukan subscribe topik: ${err.message}`, "system");
          } else {
            console.log("[Browser MQTT] Client-side subscription success!");
          }
        });
      });
      
      client.on("message", (topic, message) => {
        const value = message.toString().trim();
        console.log(`[Browser MQTT Msg] ${topic} => ${value}`);
        handleIncomingClientMqttMessage(topic, value);
      });
      
      client.on("error", (err) => {
        console.error("[Browser MQTT Error]:", err.message);
        setState(prev => ({ ...prev, brokerConnected: false }));
        addClientEvent("system", `Konfig Broker ${state.activeBrokerIdx + 1} error: ${err.message}`, "system");
      });
      
      client.on("close", () => {
        setState(prev => {
          if (prev.brokerConnected) {
            addClientEvent("broker", `Koneksi langsung browser ke Broker ${prev.activeBrokerIdx + 1} terputus.`, "system");
          }
          return { ...prev, brokerConnected: false };
        });
      });
      
    } catch (err: any) {
      console.error("[Browser MQTT Connect Exception]:", err);
      addClientEvent("system", `Inisialisasi WebSocket browser gagal: ${err.message}`, "system");
    }
    
    return () => {
      if (clientRef.current) {
        try {
          clientRef.current.end();
          clientRef.current = null;
        } catch (e) {}
      }
    };
  }, [state.activeBrokerIdx, brokers]);

  // Handle incoming topics received inside browser directly
  const handleIncomingClientMqttMessage = (topic: string, value: string) => {
    if (topic === "sensor/suhu") {
      const t = parseFloat(value);
      if (!isNaN(t)) {
        setState(prev => ({ ...prev, temperature: parseFloat(t.toFixed(1)) }));
        addClientEvent("sensor", `Suhu termonitor: ${parseFloat(t.toFixed(1))}°C`, "esp32");
      }
    } else if (topic === "sensor/kelembaban") {
      const h = parseFloat(value);
      if (!isNaN(h)) {
        setState(prev => ({ ...prev, humidity: parseFloat(h.toFixed(1)) }));
        addClientEvent("sensor", `Kelembaban termonitor: ${parseFloat(h.toFixed(1))}%`, "esp32");
      }
    } else if (topic === "status/broker") {
      addClientEvent("broker", `ESP32 status: ${value}`, "esp32");
      const parts = value.split("|");
      if (parts[0] && parts[0].startsWith("BROKER:")) {
        const idx = parseInt(parts[0].substring(7)) - 1;
        if (idx >= 0 && idx <= 2 && idx !== state.activeBrokerIdx) {
          console.log(`[Browser Sync] ESP32 has selected Broker #${idx + 1}. Syncing...`);
          addClientEvent("broker", `Browser menyelaraskan broker dengan ESP32 ke Broker ${idx + 1}`, "system");
          setState(prev => ({ ...prev, activeBrokerIdx: idx }));
        }
      }
    } else if (topic === "kontrol/relay1") {
      const nextVal = (value === "ON");
      setState(prev => {
        const nextRelays = [...prev.relays] as [boolean, boolean, boolean, boolean];
        if (nextRelays[0] !== nextVal) {
          nextRelays[0] = nextVal;
          addClientEvent("relay", `Relay 1 diatur: ${value}`, "esp32");
          return { ...prev, relays: nextRelays };
        }
        return prev;
      });
    } else if (topic === "kontrol/relay2") {
      const nextVal = (value === "ON");
      setState(prev => {
        const nextRelays = [...prev.relays] as [boolean, boolean, boolean, boolean];
        if (nextRelays[1] !== nextVal) {
          nextRelays[1] = nextVal;
          addClientEvent("relay", `Relay 2 diatur: ${value}`, "esp32");
          return { ...prev, relays: nextRelays };
        }
        return prev;
      });
    } else if (topic === "kontrol/relay3") {
      const nextVal = (value === "ON");
      setState(prev => {
        const nextRelays = [...prev.relays] as [boolean, boolean, boolean, boolean];
        if (nextRelays[2] !== nextVal) {
          nextRelays[2] = nextVal;
          addClientEvent("relay", `Relay 3 diatur: ${value}`, "esp32");
          return { ...prev, relays: nextRelays };
        }
        return prev;
      });
    } else if (topic === "kontrol/relay4") {
      const nextVal = (value === "ON");
      setState(prev => {
        const nextRelays = [...prev.relays] as [boolean, boolean, boolean, boolean];
        if (nextRelays[3] !== nextVal) {
          nextRelays[3] = nextVal;
          addClientEvent("relay", `Relay 4 diatur: ${value}`, "esp32");
          return { ...prev, relays: nextRelays };
        }
        return prev;
      });
    } else if (topic === "kontrol/variasi") {
      if (value === "STOP") {
        setState(prev => {
          if (prev.variasiMode !== 0) {
            addClientEvent("variasi", `Animasi variasi dihentikan`, "esp32");
            return { ...prev, variasiMode: 0 };
          }
          return prev;
        });
      } else {
        const mode = parseInt(value);
        if (mode === 1 || mode === 2) {
          setState(prev => {
            if (prev.variasiMode !== mode) {
              addClientEvent("variasi", `Variasi mode ${mode} diaktifkan`, "esp32");
              return { ...prev, variasiMode: mode };
            }
            return prev;
          });
        }
      }
    } else if (topic === "kontrol/variasi/jeda") {
      const jeda = parseInt(value);
      if (!isNaN(jeda) && jeda >= 50 && jeda <= 500) {
        setState(prev => {
          if (prev.variasiJeda !== jeda) {
            addClientEvent("variasi", `Jeda bervariasi diperbaharui ke ${jeda} ms`, "esp32");
            return { ...prev, variasiJeda: jeda };
          }
          return prev;
        });
      }
    } else if (topic === "kontrol/broker") {
      const idx = parseInt(value) - 1;
      if (idx >= 0 && idx <= 2 && idx !== state.activeBrokerIdx) {
        addClientEvent("broker", `Permintaan ganti broker terdeteksi ke Broker ${idx + 1}`, "esp32");
        setState(prev => ({ ...prev, activeBrokerIdx: idx }));
      }
    }
  };

  // Helper helper publish MQTT directly to connection
  const publishDirect = (topic: string, payload: string): boolean => {
    if (clientRef.current && state.brokerConnected) {
      console.log(`[Browser MQTT Publish] ${topic} => ${payload}`);
      clientRef.current.publish(topic, payload, { qos: 1 });
      
      let eventType: "system" | "sensor" | "broker" | "relay" | "variasi" | "voice" = "system";
      if (topic.includes("relay")) eventType = "relay";
      else if (topic.includes("sensor")) eventType = "sensor";
      else if (topic.includes("broker")) eventType = "broker";
      else if (topic.includes("variasi")) eventType = "variasi";
      
      addClientEvent(eventType, `Instruksi Direct Web: ${topic} => ${payload}`, "web");
      return true;
    } else {
      console.warn("[Browser MQTT] Client not connected - cannot execute direct websocket publish.");
      return false;
    }
  };

  // -------------------------------------------------------------
  //  BROWSER DIRECT SIMULATOR TELEMETRI DRIFT ENGINE
  // -------------------------------------------------------------
  useEffect(() => {
    if (!isSimulatorActive) return;
    
    console.log("[Browser Sim] Starting client-side telemetry generator...");
    const simInterval = setInterval(() => {
      // Drift values slightly
      const tempDelta = (Math.random() - 0.5) * 0.4;
      const nextTemp = parseFloat(Math.min(35, Math.max(16, state.temperature + tempDelta)).toFixed(1));
      
      const humDelta = (Math.random() - 0.5) * 1.0;
      const nextHum = parseFloat(Math.min(95, Math.max(40, state.humidity + humDelta)).toFixed(1));
      
      setState(prev => ({
        ...prev,
        temperature: nextTemp,
        humidity: nextHum,
        lastUpdated: new Date().toISOString()
      }));
      
      // Publish directly over active broker so all listening hardware receives updates!
      if (clientRef.current && state.brokerConnected) {
        clientRef.current.publish("sensor/suhu", nextTemp.toString(), { qos: 1 });
        clientRef.current.publish("sensor/kelembaban", nextHum.toString(), { qos: 1 });
      }
      
      addClientEvent("sensor", `Simulator: Suhu = ${nextTemp}°C, Kelembaban = ${nextHum}%`, "system");
    }, 5000);
    
    return () => {
      console.log("[Browser Sim] Stopping client-side telemetry generator...");
      clearInterval(simInterval);
    };
  }, [isSimulatorActive, state.temperature, state.humidity, state.brokerConnected]);

  // -------------------------------------------------------------
  //  CONTROL & API DISPATCHERS
  // -------------------------------------------------------------

  const toggleRelay = async (idx: number) => {
    const topic = `kontrol/relay${idx + 1}`;
    const nextPayload = state.relays[idx] ? "OFF" : "ON";
    
    // Snappy optimistic direct update
    const nextRelays = [...state.relays] as [boolean, boolean, boolean, boolean];
    nextRelays[idx] = !state.relays[idx];
    setState(prev => ({ ...prev, relays: nextRelays }));

    // Publish directly over WebSockets from browser for 100% responsiveness on Vercel
    const directSent = publishDirect(topic, nextPayload);

    if (!directSent) {
      try {
        await fetch("/api/control", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ topic, payload: nextPayload })
        });
      } catch (err) {
        console.warn("Failed REST toggle relay backup (expected in Vercel serverless):", err);
      }
    }
  };

  const dispatchVariasiMode = async (mode: number) => {
    const topic = "kontrol/variasi";
    const payload = mode === 0 ? "STOP" : mode.toString();

    // Snappy optimistic update
    setState(prev => ({ ...prev, variasiMode: mode }));

    // Publish directly over WebSockets
    const directSent = publishDirect(topic, payload);

    if (!directSent) {
      try {
        await fetch("/api/control", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ topic, payload })
        });
      } catch (err) {
        console.warn("Failed REST variasi mode backup (expected in Vercel):", err);
      }
    }
  };

  const dispatchVariasiJeda = async (jedaValue: number) => {
    const topic = "kontrol/variasi/jeda";
    
    // optimistic update
    setState(prev => ({ ...prev, variasiJeda: jedaValue }));

    // Publish directly over WebSockets
    const directSent = publishDirect(topic, jedaValue.toString());

    if (!directSent) {
      try {
        await fetch("/api/control", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ topic, payload: jedaValue.toString() })
        });
      } catch (err) {
        console.warn("Failed REST variasi jeda backup (expected in Vercel):", err);
      }
    }
  };

  const changeBroker = async (index: number) => {
    if (index === state.activeBrokerIdx) return;
    
    setState(prev => ({ ...prev, activeBrokerIdx: index, brokerConnected: false }));
    addClientEvent("broker", `Ganti broker dipilih ke: Broker ${index + 1}`, "web");

    // Publish switch broker command over existing broker first if connected
    if (clientRef.current && state.brokerConnected) {
      clientRef.current.publish("kontrol/broker", `${index + 1}`, { qos: 1 });
    }

    try {
      await fetch("/api/switch-broker", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ index })
      });
    } catch (err) {
      console.warn("Failed REST switch-broker backup (expected in Vercel):", err);
    }
  };

  const startEditBroker = (idx: number) => {
    const broker = brokers[idx] || {
      server: idx === 0 ? "kingfisher.lmq.cloudamqp.com" : idx === 1 ? "node02.myqtthub.com" : "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud",
      port: 8883,
      user: idx === 0 ? "wxoeelnh" : idx === 1 ? "ESP" : "Web",
      pass: idx === 0 ? "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x" : "a",
      clientId: idx === 0 ? "ESP32AMQP" : "WebClient",
      vhost: idx === 0 ? "wxoeelnh" : null
    };
    setEditingBrokerIdx(idx);
    setEditServer(broker.server || "");
    setEditPort(String(broker.port || 8883));
    setEditUser(broker.user || "");
    setEditPass(broker.pass || "");
    setEditClientId(broker.clientId || "");
    setEditVhost(broker.vhost || "");
  };

  const handleSaveBroker = async (e: React.FormEvent) => {
    e.preventDefault();
    if (editingBrokerIdx === null) return;

    const updatedBroker = {
      id: editingBrokerIdx + 1,
      server: editServer,
      port: parseInt(editPort) || 1883,
      user: editUser,
      pass: editPass,
      clientId: editClientId,
      vhost: editVhost || null
    };

    const defaultBrokers = [
      { id: 1, name: "CloudAMQP (Primary)",         server: "kingfisher.lmq.cloudamqp.com",         port: 8883, user: "wxoeelnh", pass: "BQAdo1W8qPeDlnF1O2WZ_AdUTd_uVG0x", clientId: "ESP32AMQP", vhost: "wxoeelnh" },
      { id: 2, name: "MyQtthub (Backup)",           port: 8883, server: "node02.myqtthub.com",                  user: "ESP",    pass: "a",                                 clientId: "WebClient",     vhost: null },
      { id: 3, name: "Cedalo Cloud (Fallback)",     port: 8883, server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud", user: "Web",    pass: "a",                                 clientId: "WebClient",    vhost: null }
    ];

    const currentBrokers = brokers.length > 0 ? brokers : defaultBrokers;
    const nextBrokers = [...currentBrokers];
    nextBrokers[editingBrokerIdx] = updatedBroker;

    // Cache updated broker list persistent in the browser localStorage
    setBrokers(nextBrokers);
    localStorage.setItem("mqtt_brokers", JSON.stringify(nextBrokers));
    addClientEvent("broker", `Kredensial Broker ${editingBrokerIdx + 1} diperbaharui di browser: ${editServer}:${editPort}`, "web");
    setEditingBrokerIdx(null);

    // Save on backend as a non-blocking reference
    try {
      await fetch("/api/update-broker", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          index: editingBrokerIdx,
          server: editServer,
          port: parseInt(editPort) || 1883,
          user: editUser,
          pass: editPass,
          clientId: editClientId,
          vhost: editVhost || null
        })
      });
    } catch (err) {
      console.warn("Failed REST update-broker backup (expected in Vercel):", err);
    }
  };

  const toggleSimulator = async () => {
    const nextSim = !isSimulatorActive;
    setIsSimulatorActive(nextSim);
    addClientEvent("system", `Mode simulator telemetri ESP32 ${nextSim ? "diaktifkan" : "dinonaktifkan"}.`, "system");

    try {
      await fetch("/api/simulator", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: nextSim })
      });
    } catch (err) {
      console.warn("Failed REST toggle simulator backup (expected in Vercel):", err);
    }
  };

  // -------------------------------------------------------------
  //  VOICE / SPEECH RECOGNITION ACTIONS
  // -------------------------------------------------------------

  const handleStartMic = () => {
    if (!speechSupported) return;
    if (isRecording) {
      recognitionRef.current?.stop();
    } else {
      recognitionRef.current?.start();
    }
  };

  const handleTextCommandSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!textCommand.trim()) return;
    sendVoiceCommand(textCommand);
    setTextCommand("");
  };

  const sendVoiceCommand = async (commandText: string) => {
    setIsAiProcessing(true);
    setAiResponse(null);
    setLastExecutedVoiceRules([]);

    try {
      const response = await fetch("/api/voice-command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ transcript: commandText })
      });

      if (response.ok) {
        const payload = await response.json();
        setAiResponse(payload.message);
        setLastExecutedVoiceRules(payload.commands || []);

        // Execute commands returned by AI voice!
        if (payload.commands && Array.isArray(payload.commands)) {
          payload.commands.forEach((cmd: any) => {
            if (cmd.topic && cmd.payload !== undefined) {
              const payloadStr = String(cmd.payload);
              if (cmd.topic === "kontrol/broker") {
                const bIdx = parseInt(payloadStr) - 1;
                if (bIdx >= 0 && bIdx <= 2) {
                  setState(prev => ({ ...prev, activeBrokerIdx: bIdx, brokerConnected: false }));
                  addClientEvent("broker", `Voice AI mengalihkan Broker ke #${bIdx + 1}`, "system");
                }
              } else {
                publishDirect(cmd.topic, payloadStr);
                handleIncomingClientMqttMessage(cmd.topic, payloadStr);
              }
            }
          });
        }

        // Speech text out in Indonesian
        if ("speechSynthesis" in window) {
          const speakText = payload.message;
          const utterance = new SpeechSynthesisUtterance(speakText);
          utterance.lang = "id-ID";
          utterance.pitch = 1.0;
          utterance.rate = 1.0;
          window.speechSynthesis.speak(utterance);
        }
      } else {
        setAiResponse("Koneksi gagal saat berkomunikasi dengan AI Gemini parser.");
      }
    } catch (err) {
      console.error("Voice parse error:", err);
      setAiResponse("Gagal memproses instruksi melalui server.");
    } finally {
      setIsAiProcessing(false);
    }
  };

  const copyEventToClipboard = (id: string, text: string) => {
    navigator.clipboard.writeText(text);
    setCopiedEventId(id);
    setTimeout(() => setCopiedEventId(null), 1500);
  };

  const clearLoggedEvents = () => {
    setEvents([]);
  };

  // Human friendly system format
  const formatTime = (isoString: string) => {
    try {
      const d = new Date(isoString);
      return d.toTimeString().split(' ')[0] + "." + String(d.getMilliseconds()).padStart(3, '0');
    } catch (e) {
      return "00:00:00.000";
    }
  };

  return (
    <div className="min-h-screen bg-slate-100 text-slate-800 font-sans p-4 md:p-6 lg:p-8 flex flex-col justify-between selection:bg-indigo-600 selection:text-white">
      
      {/* HEADER SECTION */}
      <header className="max-w-7xl w-full mx-auto mb-6 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4 bg-slate-900 text-white px-8 py-4.5 rounded-2xl border-b border-slate-700 shadow-md">
        <div className="flex items-center gap-4">
          <div className="bg-indigo-600 p-2.5 rounded-lg shadow-md shadow-indigo-900/30">
            <Cpu className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="font-display font-extrabold text-xl md:text-2xl tracking-tight text-white flex items-center gap-2">
              ESP32 IoT Control Center
              <span className="text-[10px] bg-slate-800 text-indigo-300 font-mono py-0.5 px-2.5 rounded-full border border-slate-700 font-bold tracking-wider">
                v2.1 TLS
              </span>
            </h1>
            <p className="text-xs text-slate-400 font-mono uppercase tracking-widest mt-0.5">
              Hardware Monitor & Smart Gateway
            </p>
          </div>
        </div>

        {/* System telemetry badges */}
        <div className="flex flex-wrap items-center gap-3">
          {/* Signal Indicator */}
          <div className="flex items-center gap-2 bg-slate-800 border border-slate-700 px-3 py-1.5 rounded-full text-xs font-semibold">
            <span className="relative flex h-2 w-2">
              {connectionStatus === "CONNECTED" && (
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
              )}
              <span className={`relative inline-flex rounded-full h-2 w-2 ${
                connectionStatus === "CONNECTED" 
                  ? "bg-emerald-500" 
                  : connectionStatus === "CONNECTING"
                  ? "bg-amber-500"
                  : "bg-rose-500"
              }`}></span>
            </span>
            <span className="text-slate-300">
              WiFi: <span className="text-white font-bold">Kocakk</span>
            </span>
          </div>

          {/* Broker Connection Status */}
          <div className="flex items-center gap-2 bg-slate-800 border border-slate-700 px-3 py-1.5 rounded-full text-xs font-semibold">
            <span className={`w-2 h-2 rounded-full ${state.brokerConnected ? "bg-indigo-400" : "bg-rose-400"}`}></span>
            <span className="text-slate-300">
              Active: <span className="text-indigo-300">
                {state.brokerConnected ? "CloudAMQP (Connected)" : "Offline Failover"}
              </span>
            </span>
          </div>

          {/* Simulated Telemetry Toggle */}
          <button 
            id="sim-toggle-btn"
            onClick={toggleSimulator}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-bold tracking-wide border transition-all ${
              isSimulatorActive 
                ? "bg-indigo-600 text-white border-indigo-500 shadow-[0_0_12px_rgba(79,70,229,0.4)]" 
                : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700 hover:text-white"
            }`}
          >
            <RefreshCw className={`w-3 h-3 ${isSimulatorActive ? "animate-spin" : ""}`} />
            {isSimulatorActive ? "Simulator ON" : "Simulate Sensors"}
          </button>
        </div>
      </header>

      {/* MAIN CONTENT BENTO GRID */}
      <main className="max-w-7xl w-full mx-auto grid grid-cols-1 lg:grid-cols-12 gap-6 flex-grow mb-6">
        
        {/* LEFT SECTION (4 COLS): Environment & Voice Commander */}
        <div className="lg:col-span-4 flex flex-col gap-6">
          
          {/* ENVIRONMENT SENSORS BLOCK */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
            <h3 className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-4">
              Environment Sensors
            </h3>
            
            <div className="flex flex-col gap-4">
              
              {/* TEMPERATURE BOX */}
              <div id="temp-gauge-card" className="bg-slate-50 p-4 rounded-lg border border-slate-100 flex items-center justify-between relative overflow-hidden">
                <div className="flex flex-col">
                  <span className="text-xs text-slate-500 font-medium">Temperature</span>
                  <span className="text-3xl font-extrabold text-slate-900 mt-1">
                    {state.temperature ? state.temperature : "28.4"}
                    <span className="text-slate-400 text-lg ml-1 font-normal">°C</span>
                  </span>
                  <span className="text-[10px] text-slate-400 mt-1 font-mono">
                    dht11 telemetry index
                  </span>
                </div>
                <div className="relative w-12 h-12 flex items-center justify-center">
                  <svg className="absolute w-full h-full transform -rotate-90">
                    <circle cx="24" cy="24" r="20" stroke="#f1f5f9" strokeWidth="3" fill="transparent" />
                    <circle cx="24" cy="24" r="20" stroke="#f97316" strokeWidth="3" fill="transparent"
                      strokeDasharray={125.6}
                      strokeDashoffset={125.6 - (125.6 * Math.min(50, state.temperature || 28.4)) / 50}
                      className="transition-all duration-1000 ease-out"
                    />
                  </svg>
                  <Thermometer className="w-5 h-5 text-orange-500 relative z-10" />
                </div>
              </div>

              {/* HUMIDITY BOX */}
              <div id="hum-gauge-card" className="bg-slate-50 p-4 rounded-lg border border-slate-100 flex items-center justify-between relative overflow-hidden">
                <div className="flex flex-col">
                  <span className="text-xs text-slate-500 font-medium">Humidity</span>
                  <span className="text-3xl font-extrabold text-slate-900 mt-1">
                    {state.humidity ? state.humidity : "64"}
                    <span className="text-slate-400 text-lg ml-1 font-normal">%</span>
                  </span>
                  <span className="text-[10px] text-slate-400 mt-1 font-mono">
                    relative humidity
                  </span>
                </div>
                <div className="relative w-12 h-12 flex items-center justify-center">
                  <svg className="absolute w-full h-full transform -rotate-90">
                    <circle cx="24" cy="24" r="20" stroke="#f1f5f9" strokeWidth="3" fill="transparent" />
                    <circle cx="24" cy="24" r="20" stroke="#3b82f6" strokeWidth="3" fill="transparent"
                      strokeDasharray={125.6}
                      strokeDashoffset={125.6 - (125.6 * Math.min(100, state.humidity || 64)) / 100}
                      className="transition-all duration-1000 ease-out"
                    />
                  </svg>
                  <Droplets className="w-5 h-5 text-blue-500 relative z-10" />
                </div>
              </div>

            </div>
          </div>

          {/* INDIGO IMMERSIVE VOICE COMMANDER CARD */}
          <div className="bg-indigo-900 rounded-xl shadow-lg p-6 flex flex-col flex-grow text-white justify-between relative overflow-hidden min-h-[300px]">
            <div className="absolute top-0 right-0 w-32 h-32 bg-indigo-800/45 rounded-full blur-2xl pointer-events-none" />
            
            <div className="flex items-center justify-between mb-4 relative z-10">
              <h3 className="text-xs font-bold text-indigo-300 uppercase tracking-wider">
                Voice Commander
              </h3>
              <span className="flex h-2 w-2 rounded-full bg-red-400 animate-pulse"></span>
            </div>

            <div className="flex-grow flex flex-col items-center justify-center gap-4 py-4 relative z-10">
              <button
                id="mic-record-btn"
                disabled={!speechSupported}
                onClick={handleStartMic}
                className={`w-20 h-20 rounded-full flex items-center justify-center transition-all cursor-pointer ${
                  isRecording 
                    ? "bg-red-500 hover:bg-red-600 border-4 border-red-300 shadow-[0_0_20px_rgba(239,68,68,0.5)] animate-pulse" 
                    : "bg-indigo-600 hover:bg-indigo-500 border-4 border-indigo-400/30 shadow-md shadow-indigo-950/20"
                }`}
              >
                {isRecording ? (
                  <MicOff className="w-8 h-8 text-white" />
                ) : (
                  <Mic className="w-8 h-8 text-white" />
                )}
              </button>
              
              <div className="text-center">
                <p className="text-indigo-100 font-bold text-sm">
                  {isRecording ? "Mendengarkan..." : "Tekan untuk Berbicara"}
                </p>
                <p className="text-[11px] text-indigo-300 mt-1 uppercase italic tracking-wider">
                  contoh: "relay satu nyala"
                </p>
              </div>
            </div>

            {/* Transcript Area */}
            <div id="ai-response-viewport" className="mt-4 bg-indigo-950/60 rounded-xl p-3 min-h-[75px] border border-indigo-800/80 text-xs">
              {isAiProcessing ? (
                <div className="flex items-center gap-2 text-indigo-300 py-1 font-mono">
                  <RefreshCw className="w-3 h-3 animate-spin" />
                  <span>Gemini me-mikirkan instruksi...</span>
                </div>
              ) : aiResponse ? (
                <div className="space-y-1">
                  {transcript && (
                    <p className="text-[10px] text-indigo-300 font-mono italic">
                      "{transcript}"
                    </p>
                  )}
                  <p className="text-white font-medium leading-relaxed">
                    {aiResponse}
                  </p>
                </div>
              ) : (
                <p className="text-xs font-mono text-indigo-300 opacity-60">
                  Antarmuka NLP Bahasa Indonesia siap...
                </p>
              )}
            </div>

            {/* Form to manuals commands if mic is disabled */}
            <form id="text-command-form" onSubmit={handleTextCommandSubmit} className="mt-3 flex gap-2 relative z-10">
              <input
                type="text"
                placeholder="Atau ketik instruksi di sini..."
                value={textCommand}
                onChange={(e) => setTextCommand(e.target.value)}
                className="flex-grow py-1.5 px-3 bg-indigo-950/70 border border-indigo-800/60 rounded-lg text-xs text-white placeholder-indigo-400/50 outline-none focus:border-indigo-400"
              />
              <button
                id="submit-text-command-btn"
                type="submit"
                className="bg-indigo-700 hover:bg-indigo-600 px-3 rounded-lg border border-indigo-600 text-xs flex items-center justify-center font-bold text-white transition-colors"
              >
                <Send className="w-3 h-3" />
              </button>
            </form>
          </div>

        </div>

        {/* RIGHT SECTION (8 COLS): Manual Overrides & Sequence broker */}
        <div className="lg:col-span-8 flex flex-col gap-6">
          
          {/* MANUAL RELAY OVERRIDES CELL */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
            <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3 mb-6">
              <div>
                <h3 className="text-sm font-bold text-slate-800 uppercase tracking-wider flex items-center gap-2">
                  <Sliders className="w-4 h-4 text-indigo-600" />
                  Manual Relay Overrides
                </h3>
                <p className="text-xs text-slate-500 mt-1">
                  Atur beban kontak langsung (diabaikan jika variasi di bawah sedang berjalan)
                </p>
              </div>

              {state.variasiMode > 0 && (
                <div className="flex items-center gap-2 text-xs font-bold bg-amber-50 text-amber-700 px-3 py-1.5 rounded-lg border border-amber-200">
                  <span className="w-2 h-2 rounded-full bg-amber-500 animate-ping"></span>
                  Manual Disabled during Animation
                </div>
              )}
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              {state.relays.map((isOn, idx) => {
                const spec = RELAY_LOADOUT_NAMES[idx];
                const isDisabledByVariasi = state.variasiMode > 0;

                return (
                  <button
                    id={`relay-btn-${idx + 1}`}
                    key={idx}
                    disabled={isDisabledByVariasi}
                    onClick={() => toggleRelay(idx)}
                    className={`flex flex-col items-center justify-between p-4 rounded-xl border-2 transition-all min-h-[130px] ${
                      isOn 
                        ? "border-indigo-600 bg-indigo-50 text-indigo-700 shadow-sm" 
                        : "border-slate-100 bg-slate-50/60 text-slate-400 hover:border-slate-200"
                    } ${isDisabledByVariasi ? "opacity-50 cursor-not-allowed" : "cursor-pointer"}`}
                  >
                    {/* Identifier */}
                    <div className={`w-9 h-9 rounded-full flex items-center justify-center font-bold text-xs ${
                      isOn ? "bg-indigo-600 text-white" : "bg-slate-200 text-slate-500"
                    }`}>
                      {idx + 1}
                    </div>

                    <div className="text-center my-2">
                      <span className="text-xs font-bold uppercase block text-slate-800">
                        Relay {idx + 1}
                      </span>
                      <span className="text-[10px] text-slate-500 truncate block max-w-[85px]">
                        {spec.title.split(":")[1] || spec.desc}
                      </span>
                    </div>

                    <span className="text-[10px] font-mono font-black uppercase tracking-wider">
                      {isOn ? "ON" : "OFF"}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>

          {/* LOWER CELL: VARIATION CONFIG & SYSTEM BROKERS */}
          <div className="grid grid-cols-1 md:grid-cols-12 gap-6 flex-grow">
            
            {/* ANIMATION VARIATION SYSTEM (6 COLS) */}
            <div className="md:col-span-6 bg-white rounded-xl border border-slate-200 shadow-sm p-6 flex flex-col justify-between">
              <div>
                <h3 className="text-sm font-bold text-slate-700 uppercase mb-4 tracking-wider">
                  Sequence Animation (Variasi)
                </h3>
                
                <div className="grid grid-cols-3 gap-2 mb-4">
                  {[
                    { value: 1, label: "Maju", desc: "1-4" },
                    { value: 2, label: "Mundur", desc: "4-1" }
                  ].map((item) => {
                    const isChosen = state.variasiMode === item.value;
                    return (
                      <button
                        id={`variasi-btn-${item.value}`}
                        key={item.value}
                        onClick={() => dispatchVariasiMode(item.value)}
                        className={`py-2 px-1.5 rounded text-xs font-bold border transition-all uppercase ${
                          isChosen
                            ? "bg-indigo-600 text-white border-indigo-700 shadow-inner"
                            : "bg-slate-100 hover:bg-slate-200 text-slate-600 border-slate-200"
                        }`}
                      >
                        {item.label}
                      </button>
                    );
                  })}

                  {/* Stop option */}
                  <button
                    id="variasi-btn-0"
                    onClick={() => dispatchVariasiMode(0)}
                    className={`py-2 px-1.5 rounded text-xs font-bold border transition-all uppercase ${
                      state.variasiMode === 0
                        ? "bg-red-600 text-white border-red-700"
                        : "bg-red-50 hover:bg-red-100 text-red-600 border-red-100"
                    }`}
                  >
                    Stop
                  </button>
                </div>

                {/* Range Delay controls */}
                <div className="space-y-3">
                  <div className="flex justify-between text-xs font-bold">
                    <label className="text-slate-500 uppercase">Step Interval (Jeda)</label>
                    <span className="text-indigo-600 px-2.5 py-0.5 bg-indigo-50 rounded font-mono border border-indigo-100">
                      {state.variasiJeda}ms
                    </span>
                  </div>
                  
                  <input
                    id="variasi-jeda-slider"
                    type="range"
                    min="50"
                    max="500"
                    step="10"
                    value={state.variasiJeda}
                    onChange={(e) => dispatchVariasiJeda(parseInt(e.target.value))}
                    className="w-full h-1.5 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-indigo-600"
                  />
                  
                  <div className="flex justify-between text-[10px] font-mono text-slate-400">
                    <span>50ms (Cepat)</span>
                    <span>500ms (Lambat)</span>
                  </div>
                </div>
              </div>

              {/* Live activity bar */}
              <div className="mt-4 p-4 bg-emerald-50 border border-emerald-100 rounded-lg">
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
                  <p className="text-xs font-bold text-emerald-800 uppercase tracking-tight">Active State</p>
                </div>
                <p className="text-xs text-emerald-700 mt-1 font-mono">
                  {state.variasiMode > 0 
                     ? `Mode ${state.variasiMode} (${state.variasiMode === 1 ? "Forward" : "Reverse"}) Running...`
                     : "Waiting manual trigger override command..."}
                </p>
              </div>
            </div>

            {/* FAILOVER NETWORK BROKER SYSTEM (6 COLS) */}
            <div className="md:col-span-6 bg-white rounded-xl border border-slate-200 shadow-sm p-6 flex flex-col justify-between">
              <div>
                <h3 className="text-sm font-bold text-slate-700 uppercase mb-4 tracking-wider">
                  Network Broker System
                </h3>

                <div className="space-y-3">
                  {(brokers.length > 0 ? brokers : [
                    { name: "CloudAMQP Premium TLS", server: "kingfisher.lmq.cloudamqp.com" },
                    { name: "MyQttHub TLS", server: "node02.myqtthub.com" },
                    { name: "Cedalo Cloud TLS", server: "pf-l6rvh5uuefqnek6dwyef.cedalo.cloud" }
                  ]).map((broker, idx) => {
                    const isActive = state.activeBrokerIdx === idx;
                    const isConnected = state.brokerConnected;
                    const defaultNames = ["CloudAMQP (Primary)", "MyQtthub (Backup)", "Cedalo Cloud (Fallback)"];
                    const nameToUse = broker.name || defaultNames[idx];

                    return (
                      <div
                        id={`broker-panel-${idx + 1}`}
                        key={idx}
                        onClick={() => changeBroker(idx)}
                        className={`flex items-center justify-between p-3 rounded-lg border-2 transition-all cursor-pointer ${
                          isActive 
                            ? "border-indigo-600 bg-indigo-50 shadow-sm" 
                            : "border-slate-100 bg-slate-50/60 opacity-80 hover:opacity-100"
                        }`}
                      >
                        <div className="flex items-center gap-3">
                          <span className={`font-mono text-xs font-black ${isActive ? "text-indigo-700" : "text-slate-400"}`}>
                            0{idx + 1}
                          </span>
                          <div className="min-w-0">
                            <p className="text-xs font-bold text-slate-900 truncate">{nameToUse}</p>
                            <p className="text-[9px] text-slate-500 font-mono truncate max-w-[150px]">{broker.server}</p>
                          </div>
                        </div>

                        <div className="flex items-center gap-2">
                          {isActive && (
                            <span className={`text-[9px] font-bold uppercase px-2 py-0.5 rounded shadow-sm border ${
                              isConnected 
                                ? "text-emerald-700 bg-emerald-100 border-emerald-200" 
                                : "text-amber-700 bg-amber-100 border-amber-200 animate-pulse"
                            }`}>
                              {isConnected ? "ACTIVE" : "CONNECT..."}
                            </span>
                          )}
                          <button
                            id={`edit-broker-btn-${idx + 1}`}
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation();
                              startEditBroker(idx);
                            }}
                            className={`p-1.5 rounded-full hover:bg-slate-200 transition-colors ${
                              editingBrokerIdx === idx ? "text-indigo-600 bg-indigo-50" : "text-slate-400 hover:text-slate-700"
                            }`}
                            title="Konfigurasi kredensial broker"
                          >
                            <Settings className="w-3.5 h-3.5" />
                          </button>
                        </div>
                      </div>
                    );
                  })}
                </div>

                {/* Inline collapse editor */}
                {editingBrokerIdx !== null && (
                  <form onSubmit={handleSaveBroker} className="mt-4 p-4 bg-slate-50 border border-slate-200 rounded-xl space-y-3 shadow-inner">
                    <div className="flex justify-between items-center border-b border-slate-200 pb-2">
                      <span className="text-xs font-bold text-slate-700 uppercase flex items-center gap-1">
                        <Settings className="w-3.5 h-3.5 text-indigo-600" />
                        Konfigurasi Broker 0{editingBrokerIdx + 1}
                      </span>
                      <button
                        type="button"
                        onClick={() => setEditingBrokerIdx(null)}
                        className="text-[10px] font-bold text-slate-400 hover:text-red-500 uppercase"
                      >
                        Batal
                      </button>
                    </div>

                    <div className="grid grid-cols-12 gap-2">
                      <div className="col-span-8">
                        <label className="text-[9px] uppercase font-bold text-slate-500 block mb-0.5">Host / Server</label>
                        <input
                          type="text"
                          value={editServer}
                          onChange={(e) => setEditServer(e.target.value)}
                          className="w-full text-xs p-2 border border-slate-200 bg-white rounded font-mono focus:outline-indigo-500"
                          placeholder="broker.hivemq.com"
                          required
                        />
                      </div>
                      <div className="col-span-4">
                        <label className="text-[9px] uppercase font-bold text-slate-500 block mb-0.5">Port</label>
                        <input
                          type="number"
                          value={editPort}
                          onChange={(e) => setEditPort(e.target.value)}
                          className="w-full text-xs p-2 border border-slate-200 bg-white rounded font-mono focus:outline-indigo-500"
                          placeholder="8883"
                          required
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="text-[9px] uppercase font-bold text-slate-500 block mb-0.5">Username</label>
                        <input
                          type="text"
                          value={editUser}
                          onChange={(e) => setEditUser(e.target.value)}
                          className="w-full text-xs p-2 border border-slate-200 bg-white rounded font-mono focus:outline-indigo-500"
                          placeholder="Opsional"
                        />
                      </div>
                      <div>
                        <label className="text-[9px] uppercase font-bold text-slate-500 block mb-0.5">Password</label>
                        <input
                          type="password"
                          value={editPass}
                          onChange={(e) => setEditPass(e.target.value)}
                          className="w-full text-xs p-2 border border-slate-200 bg-white rounded font-mono focus:outline-indigo-500"
                          placeholder="Opsional"
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="text-[9px] uppercase font-bold text-slate-500 block mb-0.5">Client ID</label>
                        <input
                          type="text"
                          value={editClientId}
                          onChange={(e) => setEditClientId(e.target.value)}
                          className="w-full text-xs p-2 border border-slate-200 bg-white rounded font-mono focus:outline-indigo-500"
                          placeholder="ESP32Client"
                        />
                      </div>
                      <div>
                        <label className="text-[9px] uppercase font-bold text-slate-500 block mb-0.5">VHost (CloudAMQP)</label>
                        <input
                          type="text"
                          value={editVhost}
                          onChange={(e) => setEditVhost(e.target.value)}
                          className="w-full text-xs p-2 border border-slate-200 bg-white rounded font-mono focus:outline-indigo-500"
                          placeholder="Opsional"
                        />
                      </div>
                    </div>

                    <button
                      type="submit"
                      className="w-full py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded text-xs font-bold uppercase transition-all shadow-sm"
                    >
                      Update Kredensial & Hubungkan
                    </button>
                    
                    <p className="text-[9px] text-slate-400 font-sans italic leading-tight">
                      *Masukkan server publik seperti broker.hivemq.com (Port: 1883 tanpa user/pass) jika ingin simulasi broker kosong di luar TLS.
                    </p>
                  </form>
                )}
              </div>

              <div className="mt-4 pt-2">
                <p className="text-[10px] text-slate-400 italic">
                  * Failover akan otomatis beralih setelah 3 kali kegagalan koneksi berturut-turut.
                </p>
              </div>
            </div>

          </div>

          {/* TELEMETRY & CHASSIS CLIs Logs (Full Width underneath bottom row in 8-col side) */}
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 flex flex-col">
            <div className="flex justify-between items-center mb-3">
              <h3 className="text-xs font-bold text-slate-700 uppercase tracking-wider flex items-center gap-2">
                <Terminal className="w-4 h-4 text-slate-500" />
                Telemetri & Event Sasis Terminal
              </h3>
              
              <button
                id="clear-logs-btn"
                onClick={clearLoggedEvents}
                className="text-[10px] flex items-center gap-1 text-slate-400 hover:text-red-500 transition-colors bg-slate-50 border border-slate-200 rounded px-2 py-1"
                title="Bersihkan Log"
              >
                <Trash2 className="w-3 h-3" />
                Clear Logs
              </button>
            </div>

            {/* Professional light theme logger matching deep slate shell */}
            <div 
              ref={logContainerRef}
              id="cli-event-logs" 
              className="bg-slate-900 text-slate-200 rounded-xl p-3 font-mono text-[10px] overflow-y-auto max-h-[180px] min-h-[140px] flex flex-col gap-1 shadow-inner border border-slate-950"
            >
              {events.length === 0 ? (
                <div className="flex-grow flex items-center justify-center text-slate-500 text-[10px] italic py-8">
                  Menunggu transmisi telemetri sasis...
                </div>
              ) : (
                events.map((evt) => {
                  let originBadgeColor = "bg-slate-800 text-slate-400 border-slate-700";
                  if (evt.origin === "esp32") originBadgeColor = "bg-indigo-950 text-indigo-400 border-indigo-900";
                  if (evt.origin === "web") originBadgeColor = "bg-slate-800 text-slate-300 border-slate-700";

                  let eventColor = "text-slate-300";
                  if (evt.type === "relay") eventColor = "text-amber-300";
                  if (evt.type === "sensor") eventColor = "text-teal-300";
                  if (evt.type === "voice") eventColor = "text-purple-300 font-semibold";
                  if (evt.type === "broker") eventColor = "text-sky-300";

                  return (
                    <div key={evt.id} className="group flex justify-between items-start border-b border-slate-800/60 pb-1 hover:bg-slate-800/30 transition-all">
                      <div className="flex flex-col gap-0.5">
                        <div className="flex items-center gap-1.5">
                          <span className="text-slate-500 text-[9px]">
                            [{formatTime(evt.time)}]
                          </span>
                          <span className={`text-[8px] uppercase font-mono px-1 py-0.5 rounded border ${originBadgeColor}`}>
                            {evt.origin}
                          </span>
                        </div>
                        <p className={`mt-0.5 ${eventColor} leading-relaxed break-all`}>
                          {evt.detail}
                        </p>
                      </div>

                      <button
                        onClick={() => copyEventToClipboard(evt.id, `[${evt.origin.toUpperCase()}] ${evt.detail}`)}
                        className="opacity-0 group-hover:opacity-100 p-0.5 text-slate-500 hover:text-white transition-opacity ml-1.5 shrink-0 whitespace-nowrap"
                      >
                        {copiedEventId === evt.id ? (
                          <span className="text-[9px] text-emerald-400">Copied!</span>
                        ) : (
                          <Copy className="w-2.5 h-2.5" />
                        )}
                      </button>
                    </div>
                  );
                })
              )}
            </div>
          </div>

        </div>

      </main>

      {/* FOOTER STATUS BAR */}
      <footer className="max-w-7xl w-full mx-auto bg-white border border-slate-200 px-6 py-3 flex flex-col md:flex-row items-center justify-between gap-3 rounded-xl mt-auto">
        <div className="flex items-center gap-4 text-xs font-semibold text-slate-500">
          <span className="flex items-center gap-1">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 inline-block animate-pulse"></span> 
            MQTT Port: 8883 (TLS Security Enforced)
          </span>
          <span className="w-px h-3 bg-slate-200 hidden md:inline"></span>
          <span className="flex items-center gap-1 font-mono">Buffer payload: 512 Bytes</span>
        </div>
        <p className="text-xs font-mono text-slate-400">ESP32-AMQP-CLIENT-ID-9921X</p>
      </footer>

    </div>
  );
}
