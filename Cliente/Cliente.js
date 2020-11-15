var zmq = require('../zeromq/node_modules/zeromq')
    , pubSock = zmq.socket('pub')
    // , subSock = zmq.socket('sub')
    , reqSock = zmq.socket('req');

const globals = require('../Global/Globals');

let config = require('./configCliente.json');

//TODO LEER DE ALGUN LADO USERID
let userId = config.userId;
let coordinadorIP = config.coordinadorIP;
let coordinadorPuerto = config.coordinadorPuerto;


//SERVER NTP
const net = require('net');

const portNTP = config.portNTP;
const NTP_IP = config.ipNTP;
const INTERVAL_NTP = 1000 * config.intervalNTP; // seconds 1
const INTERVAL_PERIODO = 1000 * config.intervalPeriodo;  //seconds 120
const i = total = config.cantOffsetsNTP;
let offsetHora = 0;
let offsetAvg = 0;

const MESSAGE_TOPIC_PREFIX = 'message';
const HEARTBEAT_TOPIC_NAME = 'heartbeat';



var arregloSockets = {}; //almacena en formato clave(ipPuerto) valor(variable del socket) los sockets para cada broker
var clientesOnline = {};
let topicoIpPuertoPub = {};

var pendingRequests = {};

{// PP
    initClient();
    //initClientNTP();
}

function initClient() {
    reqSock.on("message", cbRespuestaCoordinador);
    reqSock.connect(`tcp://${coordinadorIP}:${coordinadorPuerto}`);
    var messageInicial = {
        idPeticion: globals.generateUUID(),
        accion: globals.COD_ALTA_SUB,
        topico: `${MESSAGE_TOPIC_PREFIX}/${userId}`
    };
    socketSendMessage(reqSock,messageInicial);

    var messageHeartbeatPub = {
        idPeticion: globals.generateUUID(),
        accion: globals.COD_PUB,
        topico: HEARTBEAT_TOPIC_NAME
    }

    socketSendMessage(reqSock,messageHeartbeatPub);
}

function socketSendMessage(socket, mensaje){
    // Se guarda en pending requests el envío
    if(mensaje != null && mensaje.idPeticion) // Compruebo que tenga idPeticion 
    {
        pendingRequests[mensaje.idPeticion] = mensaje;
        console.log(`Se realiza envio: ${JSON.stringify(mensaje)}`);
        socket.send(JSON.stringify(mensaje));
    }
    else
    {
        console.error('Mensaje sin idPeticion no pudo ser enviado');
    }
}

//////////////////////////////////////////////////////////////////////////////////////
//                                CLIENT NTP                                        //                                        
//////////////////////////////////////////////////////////////////////////////////////
function enviarTiemposNTP(){
    let idIntervalo = setInterval(function () {
      if (i--) {
      //Calculo cada offset, en un intervalo determinado
          let T1 = (new Date()).getTime();
          let mensaje = {
              t1: T1.toISOString(), 
          }
          client.write(JSON.stringify(mensaje));
      } 
      else {
      //Luego de calcular los N offsets (fin del intervalo), asigno el offset del cliente
          clearInterval(idIntervalo);
          console.log('Delay promedio: ' + offsetAvg + 'ms');
          offsetHora = offsetAvg;
      }
    }, INTERVAL_NTP);
  }

function initClientNTP(){
    client.on('data', function (data) {
        
        let T4 = new Date(new Date().toISOString()).getTime();
    
        // obtenemos hora del servidor
        let T1 = new Date(data.t1).getTime();
        let T2 = new Date(data.t2).getTime();
        let T3 = new Date(data.t3).getTime();
    
        // calculamos delay de la red
        // var delay = ((T2 - T1) + (T4 - T3)) / 2;
        let offsetDelNTP = ((T2 - T1) + (T3 - T4)) / 2;
        offsetAvg += offsetDelNTP/total;
        
    
        console.log('offset red:\t\t' + offsetDelNTP + ' ms');
        console.log('---------------------------------------------------');
    });
    let client = net.createConnection(portNTP, NTP_IP, sincronizacionNTP);
}


function sincronizacionNTP(){
    //Espera 2 minutos antes de enviar una nueva peticion al servidor NTP
    setInterval(function () {
        enviarTiemposNTP();
    }, INTERVAL_PERIODO);      
}


//////////////////////////////////////////////////////////////////////////////////////
//                                      NTP                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////



//////////////////////////////////////////////////////////////////////////////////////
//                                      PUB                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////
function enviarMensaje(contenido,topico){
    if(topicoIpPuertoPub.hasOwnProperty(topico)){
        let mensaje = {
            idPeticion: globals.generateUUID(),
            emisor: userId,
            mensaje: contenido,
            fecha: getTimeNTP(),
        }
        socketSendMessage(pubSock,mensaje);
    } else {
        let mensaje = {
            idPeticion: globals.generateUUID(),
            accion: globals.COD_PUB,
            topico: topico
        }
        socketSendMessage(reqSock,mensaje);
    }
}
//////////////////////////////////////////////////////////////////////////////////////
//                                      PUB                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////

function getTimeNTP(){
    var dateObj = Date.now();

    // Add 3 days to the current date & time
    //   I'd suggest using the calculated static value instead of doing inline math
    //   I did it this way to simply show where the number came from
    dateObj += offsetHora;

    // create a new Date object, using the adjusted time
    return new Date(dateObj).toISOString();    
}

// TODO: Adaptar nuevo formato
function procesarAltaTopicos(brokers){
    brokers.forEach(broker => {
        let ipPuerto = `${broker.ip}:${broker.puerto}`;

        // Si no está en mis tópicos debo agregarlo
        if(!topicoIpPuertoPub.hasOwnProperty(broker.topico))
        {
            topicoIpPuertoPub[broker.topico] = ipPuerto;
            let socket = getSocketByURL(ipPuerto);
            socket.subscribe(broker.topico);
            socket.on('message', cbProcesaMensajeRecibido);
        }
    })
}
//El coordinador me envio Ip puerto broker (de cod 1 o cod 2)

//Con globals.COD1: Es porque quiero publicar en un topico por primera vez
//Con globals.COD2: Se ejecuta cuando se vuelve a conectar o se conecta por primera vez el cliente (triple msj)
function cbRespuestaCoordinador(replyJSON) { 

    
    console.log('Recibi mensaje del coordinador');
    
    let reply = JSON.parse(replyJSON);
    console.log("Received reply : [", reply, ']');// tiene el formato de un arreglo con 3 objetos que corresponden a 3 brokers

    if(reply && reply.exito) {
        switch (reply.accion) {
            case globals.COD_PUB:
                procesarAltaTopicos(reply.resultados.datosBroker);
                break;
            case globals.COD_ALTA_SUB:
                procesarAltaTopicos(reply.resultados.datosBroker);
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

// Llega un mensaje nuevo a un tópico al que estoy suscrito
function cbProcesaMensajeRecibido(topic, message) {
    if (topic = HEARTBEAT_TOPIC_NAME) { // lo revisamos en todos para mayor flexibilidad
        //actualizo el tiempo de conexion de alguien
        clientesOnline[message.emisor] = message.fecha;
    }
    else {
        console.log('Recibio mensaje de topico:', topic.toString(), ' - ', message.toString());
    }

}


// Obtiene el socket dado un ipPuerto
function getSocketByURL(ipPuerto) {
    if (!arregloSockets.hasOwnProperty(ipPuerto)) { //con este broker no hable nunca, lo agrego a mi lista de brokers
        let nuevoSocket = zmq.socket('sub');

        arregloSockets[ipPuerto.toString()] = nuevoSocket;
        nuevoSocket.connect(`tcp://${ipPuerto.toString()}`);
    }

    return arregloSockets[ipPuerto];
}

