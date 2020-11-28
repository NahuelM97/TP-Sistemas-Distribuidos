

const globals = require('../Global/Globals');
const pub = require('../Publicador/pub');

let config = require('./configCliente.json');
let configClientNTP = require('../Global/configClientNTP.json');


// DEBUG_MODE
const DEBUG_MODE = false;


//TODO LEER DE ALGUN LADO USERID
let userId = 'DefaultUser'; // En teoría, nunca debería quedar DefaultUser. Sin embargo, dejar un default previene males peores.
let coordinadorIP = config.coordinadorIP;
let coordinadorPuerto = config.coordinadorPuerto;


//SERVER NTP
const net = require('net');
const { REPL_MODE_STRICT } = require('repl');
const { Console } = require('console');
const { initReq } = require('../Publicador/pub');

const portNTP = configClientNTP.portNTP;
const NTP_IP = configClientNTP.ipNTP;
const INTERVAL_NTP = 1000 * configClientNTP.intervalNTP; // seconds 1
const INTERVAL_PERIODO = 1000 * configClientNTP.intervalPeriodo;  //seconds 120
const INTERVAL_ENVIO_HEARTBEAT = 1000 * config.intervalEnvioHeartbeat;
const TOLERANCIA_CLIENTE = 1000 * config.toleranciaCliente;
let i = total = configClientNTP.cantOffsetsNTP;
let offsetHora = 0;
let offsetAvg = 0;
let clientNTP;

const MESSAGE_TOPIC_PREFIX = 'message';
const GROUP_TOPIC_PREFIX = 'group';
const HEARTBEAT_TOPIC_NAME = 'heartbeat';

const ONLINE_TOLERANCE = 1000 * config.onlineTolerance; // 30 segundos como máximo es el tiempo sin heartbeat en que un usuario sigue siendo considerado como online

var clientesUltimoHeartBeat = {};



function init(myUsername) {// PP
    userId = myUsername;
    initClient();
    initClientNTP();
}

function initClient() {
    pub.initReqSocket(coordinadorIP,coordinadorPuerto);
    pub.initCbSubSocket(cbProcesaMensajeRecibido);
    var messageInicial = {
        idPeticion: globals.generateUUID(),
        accion: globals.COD_ALTA_SUB,
        topico: `${MESSAGE_TOPIC_PREFIX}/${userId}`
    };
    //Lo que manda el cliente la primera vez, pidiendole los 3 topicos de alta(ip:puerto)
    pub.solicitarBrokerSubACoordinador(messageInicial);


    //Le envia al coordinador la peticion de ip:puerto para publicar heartbeats
    intentaPublicar("", HEARTBEAT_TOPIC_NAME);
    intervalHeartbeat = setInterval(cbIntervalHeartbeat, INTERVAL_ENVIO_HEARTBEAT);

}

function cbIntervalHeartbeat() {
    intentaPublicar("", HEARTBEAT_TOPIC_NAME);
}


//////////////////////////////////////////////////////////////////////////////////////
//                               <CLIENT NTP>                                       //                                        
//////////////////////////////////////////////////////////////////////////////////////
function enviarTiemposNTP() {
    let idIntervalo = setInterval(function () {
        if (i--) {
            //Calculo cada offset, en un intervalo determinado
            let T1 = new Date();
            let mensaje = {
                t1: T1.toISOString(),
            }
            clientNTP.write(JSON.stringify(mensaje));
        }
        else {
            //Luego de calcular los N offsets (fin del intervalo), asigno el offset del cliente
            clearInterval(idIntervalo);
            debugConsoleLog('Delay promedio: ' + offsetAvg + 'ms');
            offsetHora = offsetAvg;
            i = configClientNTP.cantOffsetsNTP;
        }
    }, INTERVAL_NTP);
}

function initClientNTP() {
    clientNTP = net.createConnection(portNTP, NTP_IP, sincronizacionNTP);
    clientNTP.on('data', function (dataJSON) {
        let data = JSON.parse(dataJSON);
        let T4 = new Date().getTime();

        // obtenemos hora del servidor
        let T1 = new Date(data.t1).getTime();
        let T2 = new Date(data.t2).getTime();
        let T3 = new Date(data.t3).getTime();

        // calculamos delay de la red
        // var delay = ((T2 - T1) + (T4 - T3)) / 2;
        let offsetDelNTP = ((T2 - T1) + (T3 - T4)) / 2;
        offsetAvg += offsetDelNTP / total;


        debugConsoleLog('offset red:\t\t' + offsetDelNTP + ' ms');
        debugConsoleLog('---------------------------------------------------');
    });

}


function sincronizacionNTP() {
    //Espera 2 minutos antes de enviar una nueva peticion al servidor NTP
    setInterval(function () {
        enviarTiemposNTP();
        debugConsoleLog('Sincronizacion de tiempo NTP Comenzada')
    }, INTERVAL_PERIODO);
}

//sector terminar bien las conexiones TCP
process.on('SIGHUP', function () {
    console.log('Cerrando broker');
    endClientNTP();

});

process.on('SIGINT', function () {
    console.log('Cerrando broker');
    endClientNTP();

});

async function endClientNTP() {
    await clientNTP.end();
    clientNTP.on('end', () => process.exit());
}


//////////////////////////////////////////////////////////////////////////////////////
//                                     </NTP>                                       //                                        
//////////////////////////////////////////////////////////////////////////////////////



//////////////////////////////////////////////////////////////////////////////////////
//                                      PUB                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////

// Trata de enviar un mensaje, creandolo a partir de un contenido, en un topico (broker).
//  - Si tiene el ip:puerto almacenado del broker, publica
//  - Sino, le envia una solicitud al coordinador para obtener esos datos
//30faab00-2339-4e57-928a-b78cabb4af6c
function intentaPublicar(contenido, topico) {
    //Si tengo la ubicacion del topico (broker) guardada, lo envio
    pub.intentaPublicarNuevoMensaje(userId, contenido, topico, getTimeNTP());
}


//////////////////////////////////////////////////////////////////////////////////////
//                                    </PUB>                                        //                                        
//////////////////////////////////////////////////////////////////////////////////////

//esta funcion devuelve el tiempo actual, ya corregido con el offset obtenido por NTP
function getTimeNTP() {
    let milisActual = new Date().getTime();

    milisActual += offsetHora;
    
    return new Date(milisActual).toISOString();
}




// Llega un mensaje nuevo a un tópico al que estoy suscrito
function cbProcesaMensajeRecibido(topic, messageJSON) {
    let message = JSON.parse(messageJSON);
    if (topic == HEARTBEAT_TOPIC_NAME) { // lo revisamos en todos para mayor flexibilidad
        //actualizo el tiempo de conexion de alguien
        clientesUltimoHeartBeat[message.emisor] = message.fecha;
        debugConsoleLog(`Me llego un heartbeat  con fecha ${message.fecha} y de ${message.emisor}`)
    }
    else {
        debugConsoleLog('Recibio mensaje de topico:', topic.toString(), ' - ', message.toString());

        mostrarMensajeInterfaz(getFormattedMessage(message));
    }
}

function getFormattedMessage(mensaje) {
    let formattedFecha = getFormattedDate(mensaje.fecha);
    return `${formattedFecha} - ${mensaje.emisor}: ${mensaje.mensaje}`;
}

function getFormattedDate(date) {
    let fecha = new Date(date);
    let formattedDay = fecha.getDay().toString().length == 1 ? "0" + fecha.getDay() : fecha.getDay(); // Si el día no empieza en 0 se lo agrega
    let formattedMonth = fecha.getMonth().toString().length == 1 ? "0" + fecha.getMonth() : fecha.getMonth(); // Si el mes no empieza en 0 se lo agrega
    let formattedHours = fecha.getHours().toString().length == 1 ? "0" + fecha.getHours() : fecha.getHours();
    let formattedMinutes = fecha.getMinutes().toString().length == 1 ? "0" + fecha.getMinutes() : fecha.getMinutes();
    let formattedSeconds = fecha.getSeconds().toString().length == 1 ? "0" + fecha.getSeconds() : fecha.getSeconds();
    let formattedMs = fecha.getMilliseconds().toString().length == 1 ? "00" + fecha.getMilliseconds() : (fecha.getMilliseconds().toString().length == 2 ? "0" + fecha.getMilliseconds() : fecha.getMilliseconds());
    let formattedFecha = formattedDay + '/' + formattedMonth + '/' + fecha.getFullYear() + ' ' + formattedHours + ':' + formattedMinutes + ':' + formattedSeconds + '.' + formattedMs;
    return formattedFecha;
}


// Precondicion: está registrado en clientesUltimoHeartbeat
function isUserOnline(user) {
    const msNTP = new Date(getTimeNTP()).getTime();
    const msHeartbeat = new Date(clientesUltimoHeartBeat[user]).getTime();

    debugConsoleLog(`El user es ${user}, y la diferencia da ${msNTP - msHeartbeat}`)
    return msNTP - msHeartbeat <= ONLINE_TOLERANCE;

}

function debugConsoleLog(message) {
    if (DEBUG_MODE) {
        console.log(message);
    }
}

// Importante: esto se muestra directamente en la interfaz de usuario. No debe llamarse a este método por nada que no sea algo que deba ser visto por el usuario
function mostrarMensajeInterfaz(message) {
    process.stdout.write(`\n\n${message}\n\nWASAP\\${userId}>`);
}


/**************************************************************************** /
/                                                                             /
/                            Cliente como módulo                              /
/                                                                             /
/ ****************************************************************************/

function enviarMensajeAll(contenido) {
    intentaPublicar(contenido, MESSAGE_TOPIC_PREFIX + '/all');
    return 'Mensaje de difusión enviado';
}

function enviarMensajeGrupo(contenido, idGrupo) {
    intentaPublicar(contenido, GROUP_TOPIC_PREFIX + '/' + idGrupo);
    return 'Mensaje enviado al grupo ' + idGrupo;
}

function enviarMensajeUsuario(contenido, idUsuario) {
    if (!clientesUltimoHeartBeat.hasOwnProperty(idUsuario)) {//ETOP
        return 'Se ha intentado enviar un mensaje a un usuario que no está registrado.';
    }
    if (!isUserOnline(idUsuario)) {
        return 'El usuario no está en línea.';
    }
    else {
        intentaPublicar(contenido, MESSAGE_TOPIC_PREFIX + '/' + idUsuario);
        return 'Mensaje enviado al usuario ' + idUsuario;
    }
}

function suscripcionAGrupo(idGrupo) {
    var suscripcionAGrupo = {
        idPeticion: globals.generateUUID(),
        accion: globals.COD_ALTA_SUB,
        topico: `${GROUP_TOPIC_PREFIX}/${idGrupo}`
    };
    //Lo que manda el cliente la primera vez, pidiendole los 3 topicos de alta(ip:puerto)
    pub.solicitarBrokerSubACoordinador(suscripcionAGrupo);
}


module.exports = {
    // funciones
    enviarMensajeAll: enviarMensajeAll,
    enviarMensajeGrupo: enviarMensajeGrupo,
    enviarMensajeUsuario: enviarMensajeUsuario,
    suscripcionAGrupo: suscripcionAGrupo,
    endClientNTP: endClientNTP,
    init: init
}
