console.log("Hello world!");

// Підключення до обраного порту 5000
const ws = new WebSocket("ws://localhost:5000");

ws.onopen = (e) => {
  console.log("Hello WebSocket!");
};

ws.onmessage = (e) => {
  console.log(e.data);
  const text = e.data;

  const elMsg = document.createElement("div");
  elMsg.style.whiteSpace = "pre-wrap";
  elMsg.style.marginBottom = "10px";
  elMsg.textContent = text;

  const subscribe = document.getElementById("subscribe");
  if (subscribe) {
    subscribe.appendChild(elMsg);
  }
};

ws.onerror = (error) => {
  console.error("WebSocket Error: ", error);
};

// Навішуємо подію безпосередньо через пошук форми в DOM
document.addEventListener("DOMContentLoaded", () => {
  const formChat = document.getElementById("formChat");
  const textField = document.getElementById("textField");

  if (formChat) {
    formChat.addEventListener("submit", (e) => {
      // Це блокує будь-яке очищення чи перезавантаження сторінки в першу ж мілісекунду
      e.preventDefault();

      if (textField && textField.value.trim() !== "") {
        // Перевіряємо чи сокет відкритий перед відправкою
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(textField.value);
          textField.value = "";
        } else {
          alert("Помилка: WebSocket з'єднання закрите або ще не встановилося.");
        }
      }
    });
  }
});
