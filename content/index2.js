var deviceId = localStorage.getItem('device-id');
if (deviceId === null) {
  deviceId = crypto.randomUUID();
  localStorage.setItem('device-id', deviceId);
}

// Initialize the elm app
var app = Elm.Main.init({
  node: document.querySelector("main"),
  flags: {
    deviceId: deviceId,
    window: {
      height: window.innerHeight,
      width: window.innerWidth,
    },
  }
});

app.ports.watchPosition.subscribe(
  () => {
    navigator.geolocation.watchPosition(
      (position) => {
        app.ports.currentPosition.send({
          latitude: position.coords.latitude,
          longitude: position.coords.longitude,
          timestamp: position.timestamp
        });
      },
      (err) => {
        app.ports.currentPositionError.send(err);
      },
      { enableHighAccuracy: true }
    );
  }
);

document.addEventListener(
  "visibilitychange",
  () => {
    app.ports.hidden.send(document.hidden);
  }
);
