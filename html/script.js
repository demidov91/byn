var model_eur = document.getElementsByClassName("js-index-current--eur")[0];
var model_rub = document.getElementsByClassName("js-index-current--rub")[0];
var model_uah = document.getElementsByClassName("js-index-current--uah")[0];
var model_dxy = document.getElementsByClassName("js-index-current--dxy")[0];
var model_predicted_usd = document.getElementsByClassName("js-index-predict--usd-byn")[0];
var model_predicted_eur = document.getElementsByClassName("js-index-predict--usd-eur")[0];
var model_predicted_rub = document.getElementsByClassName("js-index-predict--usd-rub")[0];
var model_last_update = document.getElementsByClassName("js-index-updated--ago")[0];
var model_last_update_time = undefined;


function index(){
    let url = (
        ((window.location.protocol === "https:") ? "wss://" : "ws://") +
        window.location.host + "/predict.ws"
    );

    _runWebsocket({url: url});
    _lastUpdateWatcher();
};


function _runWebsocket({url}){
    const socket = new WebSocket(url);

    socket.addEventListener('open', function (event) {
        console.log("Connected to ws.");
    });

    socket.addEventListener('close', function (event) {
        console.log("Connected closed.");
        setTimeout(() => {_runWebsocket({url: url});}, 2000);
    });

    socket.addEventListener('message', function (event) {
        let data = JSON.parse(event.data);

        if (data["type"] == "prediction"){
            _processTotalPrediction(data);
            model_last_update_time = new Date();
        } else {
            console.log("Unexpected data type: ", data);

        }
    });
};


function _processTotalPrediction({external, predicted}){
    _processExternalData(external);
    _processPredictedData(predicted);
}


function _processExternalData({eur, rub, uah, dxy}) {
    model_eur.textContent = eur;
    model_rub.textContent = rub;
    model_uah.textContent = uah;
    model_dxy.textContent = dxy;
}

function _processPredictedData({predicted, ridge_info, std}) {
    model_predicted_usd.textContent = parseFloat(predicted).toFixed(5);
    model_predicted_eur.textContent = parseFloat(predicted * model_eur.textContent).toFixed(5);
    model_predicted_rub.textContent = parseFloat(predicted / model_rub.textContent * 100).toFixed(5);
}


function _lastUpdateWatcher(){
    if (model_last_update_time !== undefined){
        let interval = new Date() - model_last_update_time;
        if (interval < 5000){
            model_last_update.textContent = "";
        } else if (interval < 60000) {
            model_last_update.textContent = " " + Math.round(interval / 1000) + " s. ago";
        } else {
            model_last_update.textContent = " " + Math.round(interval / 60000) + " min. ago";
        }
    }

    setTimeout(_lastUpdateWatcher, 5000);
}


index();
