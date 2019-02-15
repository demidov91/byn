function index(){
    _runWebsocket({url: "ws://localhost:5000/predict.ws"});
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
        } else {
            console.log("Unexpected data type: ", data);

        }
    });
};


function _processTotalPrediction({external, predicted}){
    _processExternalData(external);
    _processPredictedData(predicted);
}

var model_eur = document.getElementsByClassName("js-index-current--eur")[0];
var model_rub = document.getElementsByClassName("js-index-current--rub")[0];
var model_uah = document.getElementsByClassName("js-index-current--uah")[0];
var model_dxy = document.getElementsByClassName("js-index-current--dxy")[0];
var model_predicted_usd = document.getElementsByClassName("js-index-predict--usd-byn")[0];
var model_predicted_eur = document.getElementsByClassName("js-index-predict--usd-eur")[0];
var model_predicted_rub = document.getElementsByClassName("js-index-predict--usd-rub")[0];

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


index();