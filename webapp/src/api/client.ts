import axios from "axios";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || "/",
  timeout: 8000,
});

export function setAuthToken(token: string | null) {
  if (!token) {
    delete api.defaults.headers.common.Authorization;
    return;
  }
  api.defaults.headers.common.Authorization = `Bearer ${token}`;
}

export function setLang(lang: string | null) {
  if (!lang) {
    delete api.defaults.headers.common["X-TWA-Lang"];
    delete api.defaults.headers.common["Accept-Language"];
    return;
  }
  api.defaults.headers.common["X-TWA-Lang"] = lang;
  api.defaults.headers.common["Accept-Language"] = lang;
}

export default api;
