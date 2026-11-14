# Menghubungkan AI Browser ke Klien Lokal (Ollama / OpenAI Compatible)

Snapgent V3 kini dilengkapi dengan **Local AI API** yang berjalan di port `11434`. Ini memungkinkan Anda menggunakan AI Chat gratis di browser (seperti ChatGPT, Gemini, Claude) seolah-olah itu adalah model lokal (seperti Ollama atau LM Studio).

## Cara Penggunaan
1. Pastikan Bridge Snapgent sudah berjalan (`start.bat`).
2. Buka tab AI chat di browser (misal: Gemini atau ChatGPT).
3. Arahkan klien AI lokal Anda (OpenClaw, Hermes, DeepSeek Harness, dll) ke endpoint:
   - **OpenAI Compatible:** `http://127.0.0.1:11434/v1`
   - **Ollama Compatible:** `http://127.0.0.1:11434`
4. Pilih model: `auto`, `gemini-web`, `chatgpt-web`, `claude-web`, atau `deepseek-web`.

## Kenapa ini keren? (Visi B)
Anda tidak perlu membayar API key. Snapgent akan mem-forward prompt dari klien AI lokal Anda ke tab browser, mengetikkannya secara otomatis, menunggu jawabannya, dan mengembalikannya ke klien lokal Anda dalam format JSON standar. AI Browser Anda kini berfungsi sebagai *Local Inference Engine*.