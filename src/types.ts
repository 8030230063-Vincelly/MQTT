export interface BrokerConfig {
  server: string;
  port: number;
  user: string;
  pass: string;
  clientId: string;
  vhost: string | null;
}

export interface SystemState {
  relays: [boolean, boolean, boolean, boolean]; // True for ON, False for OFF
  variasiMode: number; // 0 = STOP/OFF, 1 = Maju, 2 = Mundur
  variasiJeda: number; // 50 - 500 ms
  activeBrokerIdx: number; // 0 - 2
  brokerConnected: boolean;
  temperature: number; // in Celsius
  humidity: number; // in Percent
  lastUpdated: string;
}

export interface ActivityEvent {
  id: string;
  time: string;
  type: "relay" | "sensor" | "variasi" | "broker" | "voice" | "system";
  detail: string;
  origin: "esp32" | "web" | "system";
}

export interface VoiceCommandResponse {
  commands: {
    topic: string;
    payload: string;
  }[];
  message: string;
}
