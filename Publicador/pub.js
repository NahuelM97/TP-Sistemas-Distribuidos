const zmq = require('../zeromq/node_modules/zeromq');
const globals = require('../Global/Globals');


var pubSocket = zmq.socket('pub')
, reqSocket = zmq.socket('req')
, subSocket = zmq.socket('sub');



//Es un clave:valor
//    - clave: idPeticion
//    - valor: { idPeticion: globals.generateUUID(), accion: globals.COD_PUB, topico: topico }
var pendingRequests = {};

//Es un clave:valor
//    - clave: idPeticion (mismo que en pendingRequest)
//    - valor: {idPeticion: globals.generateUUID(),emisor: userId, mensaje: contenido, fecha: *}
//
// *La fecha no se guarda, porque se calcula y se guarda antes de enviar el mensaje
var pendingPublications = {};

// Guardamos todas las conexiones abiertas para evitar hacer connect a ippuerto que ya nos conectamos
let conexiones = [];

let topicoIpPuertoPub = {};


const DEBUG_MODE = false;


//////////////////////////////////////////////////////////////////////////////////////
//                                      PUB                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////

// Trata de enviar un mensaje, creandolo a partir de un contenido, en un topico (broker).
//  - Si tiene el ip:puerto almacenado del broker, publica
//  - Sino, le envia una solicitud al coordinador para obtener esos datos
//30faab00-2339-4e57-928a-b78cabb4af6c

function initReqSocket(ip,puerto){
    reqSocket.on('message', cbRespuestaCoordinador);
    reqSocket.connect(`tcp://${ip}:${puerto}`);
}


// setea el callback
function initCbSubSocket(cbProcesaMensajeRecibido){
    subSocket.on('message', cbProcesaMensajeRecibido);
}



//El coordinador me envio Ip puerto broker (de cod 1 o cod 2)

//Con globals.COD1: Es porque quiero publicar en un topico por primera vez
//Con globals.COD2: Se ejecuta cuando se vuelve a conectar o se conecta por primera vez el cliente (triple msj)
function cbRespuestaCoordinador(replyJSON) {
    debugConsoleLog('Recibi mensaje del coordinador');

    let reply = JSON.parse(replyJSON);
    debugConsoleLog("Received reply : [" + replyJSON + ']');// tiene el formato de un arreglo con 3 objetos que corresponden a 3 brokers

    if (reply && reply.exito) {
        switch (reply.accion) {
            case globals.COD_PUB:
                enviarMensajePendiente(reply);
                break;
            case globals.COD_ALTA_SUB:
                suscribirseABroker(reply.resultados.datosBroker);
                break;
            default:
                console.error("globals.CODIGO INVALIDO DE RESPUESTA EN CLIENTE");
                break;
        }
    }
    else {
        console.error("Respuesta sin exito: " + reply.error.codigo + ' - ' + reply.error.mensaje);
    }
}

// TODO: Adaptar nuevo formato
function suscribirseABroker(brokers) {
    brokers.forEach(broker => {
        let ipPuerto = `${broker.ip}:${broker.puerto}`;
        subSocket.connect(`tcp://${ipPuerto.toString()}`);
        subSocket.subscribe(broker.topico);

        debugConsoleLog("Me suscribo a: " + broker.topico + " con IPPUERTO " + ipPuerto.toString());

    })
}



function intentaPublicarNuevoMensaje(userId, contenido, topico, fechaActual) {
    //Si tengo la ubicacion del topico (broker) guardada, lo envio
    let mensajePub = {
        emisor: userId,
        mensaje: contenido,
        fecha: fechaActual
    }
    if (topicoIpPuertoPub.hasOwnProperty(topico)) {
        publicaEnBroker(mensajePub, topico);
    } else { //Si no lo tengo, se lo pido al coordinador
        let mensajeReq = {
            idPeticion: globals.generateUUID(),
            accion: globals.COD_PUB,
            topico: topico
        }
        solicitarBrokerPubACoordinador(mensajeReq, mensajePub);

    }
}

function intentaPublicarMensajeDeCola(mensaje, topico) {
    //Si tengo la ubicacion del topico (broker) guardada, lo envio  
    console.log(topico.split('/')[1]); 
    if (topicoIpPuertoPub.hasOwnProperty(topico)) {
        console.log('entra para publicar');
        publicaEnBroker(mensaje, topico);
        
    } else { //Si no lo tengo, se lo pido al coordinador
        let mensajeReq = {
            idPeticion: globals.generateUUID(),
            accion: globals.COD_PUB,
            topico: topico
        }
        solicitarBrokerPubACoordinador(mensajeReq, mensaje);
        console.log(`pide topico ${topico}`);
        console.log(topicoIpPuertoPub);
    }
}

//Dado un mensaje, realiza el envio por pubSocket
//  el mensaje es guardado en pendingRequest, para cuando el coordinador nos responda
//  podamos saber que queriamos mandar y que topico
function solicitarBrokerPubACoordinador(mensajeReq, mensajePub) {
    pendingRequests[mensajeReq.idPeticion] = mensajeReq;
    pendingPublications[mensajeReq.idPeticion] = mensajePub;
    socketSendMessage(reqSocket, JSON.stringify(mensajeReq));
}

function conectarseParaPub(ipPuerto) {
    if (!conexiones.includes(ipPuerto)) {
        pubSocket.connect(`tcp://${ipPuerto}`);
        conexiones.push(ipPuerto);
    }
}

//Dado un mensaje, realiza el envio por pubSocket
function publicaEnBroker(mensaje, topico) {
    let mensajePub = [topico, JSON.stringify(mensaje)];
    console.log(mensajePub);
    socketSendMessage(pubSocket, mensajePub);
}

//Dado un socket y un mensaje, realiza el socket.send()
function socketSendMessage(socket, mensaje) {
    // Se guarda en pending requests el envío
    if (mensaje != null) {
        debugConsoleLog(`Se realiza envio: ${JSON.stringify(mensaje)}`);
        socket.send(mensaje);
    }
    else {
        console.error('Mensaje sin idPeticion no pudo ser enviado');
    }
}

function enviarMensajePendiente(reply){
    let broker = reply.resultados.datosBroker[0];
    let ipPuerto = `${broker.ip}:${broker.puerto}`;
    conectarseParaPub(ipPuerto);

    //!!!MUY FEO: DEPENDE DE LA RED, HARDWARE TODO!!!!
    //TODO
    setTimeout(() => {

        //conseguir el mensaje y topico que queriamos enviar
        let mensaje = pendingPublications[reply.idPeticion];
        let topico = pendingRequests[reply.idPeticion].topico;
        delete pendingPublications[reply.idPeticion];
        delete pendingRequests[reply.idPeticion];

        topicoIpPuertoPub[topico] = ipPuerto;
        publicaEnBroker(mensaje, topico);
    }, 200);
}

function solicitarBrokerSubACoordinador(mensajeReq) {
    pendingRequests[mensajeReq.idPeticion] = mensajeReq;
    socketSendMessage(reqSocket, JSON.stringify(mensajeReq));
}



//////////////////////////////////////////////////////////////////////////////////////
//                                    </PUB>                                        //                                        
//////////////////////////////////////////////////////////////////////////////////////


function debugConsoleLog(message) {
    if (DEBUG_MODE) {
        console.log(message);
    }
}

module.exports = {
    //variables
    reqSocket: reqSocket,
    pendingPublications: pendingPublications,

    // funciones
    initReqSocket: initReqSocket,
    publicaEnBroker: publicaEnBroker,
    conectarseParaPub: conectarseParaPub,
    intentaPublicarNuevoMensaje: intentaPublicarNuevoMensaje,
    intentaPublicarMensajeDeCola:intentaPublicarMensajeDeCola,
    solicitarBrokerSubACoordinador: solicitarBrokerSubACoordinador,
    solicitarBrokerPubACoordinador: solicitarBrokerPubACoordinador,
    initCbSubSocket: initCbSubSocket
}
