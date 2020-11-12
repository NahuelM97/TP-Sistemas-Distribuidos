var zmq = require('zeromq')
    , pubSock = zmq.socket('pub')
    // , subSock = zmq.socket('sub')
    , reqSock = zmq.socket('req');

let userId = 'carlitos';
let coordinadorIP = '127.0.0.1';
let coordinadorPuerto = 1234;


//SERVER NTP
const net = require('net');

const portNTP = 4444;
const NTP_IP = '127.0.0.1'
const INTERVAL_NTP = 1000; // seconds
const INTERVAL_PERIODO = 120000;
const i = total = 10;
let offsetHora = 0;
let offsetAvg = 0;

const MESSAGE_TOPIC_PREFIX = 'message';
const HEARTBEAT_TOPIC_NAME = 'heartbeat';

//CLIENTE -> COORDINADOR
const COD_PUB = 1; // Cliente publica un nuevo mensaje

const COD_ALTA_SUB = 2; // Cliente se suscribe a un tópico


//COORDINADOR -> BROKER
const COD_ADD_TOPICO_BROKER = 3;// Coordinador informa al broker un nuevo topico 


//SERVIDOR HTTP -> BROKER
const COD_GET_TOPICOS = 4; // Servidor solicita a todos los sus topicos 

const COD_BORRAR_MENSAJES = 6; // Servidor solicita a un broker que borre sus mensajes

//SERVIDOR HTTP -> TODOS LOS BROKERS
const COD_GET_MENSAJES_COLA = 5;// Servidor solicita a todos los brokers todos sus mensajes

var arregloSockets = {}; //almacena en formato clave(ipPuerto) valor(variable del socket) los sockets para cada broker
var clientesOnline = {};
let topicoIpPuertoPub = {};

var pendingRequests = {};

{// PP
    initClient();
    initClientNTP();
}

function initClient() {
    reqSock.on("message", cbRespuestaCoordinador);
    reqSock.connect(`tcp://${coordinadorIP}:${coordinadorPuerto}`);
    var messageInicial = {
        idPeticion: generateUUID(),
        accion: COD_ALTA_SUB,
        topico: `${MESSAGE_TOPIC_PREFIX}/${userId}`
    };
    socketSendMessage(reqSock,messageInicial);

    var messageHeartbeatPub = {
        idPeticion: generateUUID(),
        accion: COD_PUB,
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
            idPeticion: generateUUID(),
            emisor: userId,
            mensaje: contenido,
            fecha: getTimeNTP(),
        }
        socketSendMessage(pubSock,mensaje);
    } else {
        let mensaje = {
            idPeticion: generateUUID(),
            accion: COD_PUB,
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

//Con COD1: Es porque quiero publicar en un topico por primera vez
//Con COD2: Se ejecuta cuando se vuelve a conectar o se conecta por primera vez el cliente (triple msj)
function cbRespuestaCoordinador(replyJSON) { 
    
    let reply = JSON.parse(replyJSON);
    console.log("Received reply : [", reply, ']');// tiene el formato de un arreglo con 3 objetos que corresponden a 3 brokers

    if(reply && reply.exito) {
        switch (reply.accion) {
            case COD_PUB:
                procesarAltaTopicos(reply.resultados);
                break;
            case COD_ALTA_SUB:
                procesarAltaTopicos(reply.resultados);
                break;
            default:
                console.error("CODIGO INVALIDO DE RESPUESTA EN CLIENTE");
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

// TO DO: Abstraer a global.js
// Genera UUID a fin de ser utilizado como ID de mensaje.
function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
}
