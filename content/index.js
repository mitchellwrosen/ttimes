if (!dash) {
  var dash = (() => {
    var dash = {
      deviceId: localStorage.getItem("device-id"),
      fetched: null,
      latitude: null,
      longitude: null,
      timestamp: null,
      tzoffset: new Date().getTimezoneOffset()
    };
    if (!dash.deviceId) {
      dash.deviceId = crypto.randomUUID();
      localStorage.setItem("device-id", dash.deviceId);
    }
    function savePosition(position) {
      dash.latitude = position.coords.latitude;
      dash.longitude = position.coords.longitude;
      dash.timestamp = position.timestamp;
    }
    function afterDomContentLoaded(f) {
      if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", f, { once: true });
      } else {
        f();
      }
    }
    afterDomContentLoaded(() => {
      document.querySelector("#status").innerText = "waiting for position";
      navigator.geolocation.getCurrentPosition(
        (position) => {
          savePosition(position);
          navigator.geolocation.watchPosition(savePosition, undefined, { enableHighAccuracy: true });
          dash.fetched = Date.now();
          document.querySelector("#status").innerText = "";
          document.querySelector("#refresh-icon").dispatchEvent(new Event("dash:refresh"));
          function startTicking() {
            return window.setInterval(
              () => {
                const refreshIcon = document.querySelector("#refresh-icon");
                if (refreshIcon) {
                  refreshIcon.dispatchEvent(
                    new CustomEvent(
                      "dash:tick",
                      { detail: { timestamp: Date.now() } }
                    )
                  );
                }
              },
              1000
            );
          }
          let intervalId = startTicking();
          document.addEventListener(
            "visibilitychange",
            () => {
              if (document.hidden) {
                clearInterval(intervalId);
              } else {
                intervalId = startTicking();
              }
            }
          );
        },
        (err) => {
          document.querySelector("#status").innerText = "";
          let alertDiv = document.createElement("div");
          alertDiv.id = "alert";
          if (err.code === 1) {
            alertDiv.appendChild(
              document.createTextNode(
                "Uh oh, I couldn't get your position: \"" + err.message + "\". Please grant geolocation permission."
              )
            );
          } else {
            alertDiv.appendChild(
              document.createTextNode(
                "Uh oh, I couldn't get your position: \"" + err.message + "\"."
              )
            );
          }
          let main = document.querySelector("#main");
          main.insertBefore(alertDiv, main.firstChild);
        },
        { enableHighAccuracy: true }
      );
    });
    return dash;
  })();
}
