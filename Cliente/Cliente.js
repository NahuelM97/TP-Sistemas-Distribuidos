var zmq = require('../zeromq/node_modules/zeromq')
    , pubSocket = zmq.socket('pub')
    // , subSock = zmq.socket('sub')
    , reqSocket = zmq.socket('req');

const globals = require('../Global/Globals');

let config = require('./configCliente.json');

//TODO LEER DE ALGUN LADO USERID
let userId = config.userId;
let coordinadorIP = config.coordinadorIP;
let coordinadorPuerto = config.coordinadorPuerto;


//SERVER NTP
const net = require('net');
const { REPL_MODE_STRICT } = require('repl');

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

{// PP
    initClient();
    //initClientNTP();
}

function initClient() {
    reqSocket.on("message", cbRespuestaCoordinador);
    reqSocket.connect(`tcp://${coordinadorIP}:${coordinadorPuerto}`);
    var messageInicial = {
        idPeticion: globals.generateUUID(),
        accion: globals.COD_ALTA_SUB,
        topico: `${MESSAGE_TOPIC_PREFIX}/${userId}`
    };
    //Lo que manda el cliente la primera vez, pidiendole los 3 topicos de alta(ip:puerto)
    solicitarBrokerSubACoordinador(messageInicial);

    
    //Le envia al coordinador la peticion de ip:puerto para publicar heartbeats
    
    intentaPublicar("",HEARTBEAT_TOPIC_NAME);
}

function solicitarBrokerSubACoordinador(mensajeReq){
    pendingRequests[mensajeReq.idPeticion] = mensajeReq; 
    socketSendMessage(reqSocket, JSON.stringify(mensajeReq));
}

//Dado un mensaje, realiza el envio por subSocket
//  el mensaje es guardado en pendingRequest, para cuando el coordinador nos responda
//  podamos saber que queriamos mandar
function solicitarBrokerPubACoordinador(mensajeReq, mensajePub){
    pendingRequests[mensajeReq.idPeticion] = mensajeReq;  
    pendingPublications[mensajeReq.idPeticion] = mensajePub;
    socketSendMessage(reqSocket, JSON.stringify(mensajeReq));
}

//Dado un mensaje, realiza el envio por pubSocket
function publicaEnBroker(mensaje, topico){
    let mensajePub = [topico,JSON.stringify(mensaje)];
    socketSendMessage(pubSocket, mensajePub);
}

//Dado un socket y un mensaje, realiza el socket.send()
function socketSendMessage(socket, mensaje){
    // Se guarda en pending requests el envío
    if(mensaje != null)
    {   
        //pendingRequests[mensaje.idPeticion] = mensaje;     
        console.log(`Se realiza envio: ${JSON.stringify(mensaje)}`);
        socket.send(mensaje);
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

// Trata de enviar un mensaje, creandolo a partir de un contenido, en un topico (broker).
//  - Si tiene el ip:puerto almacenado del broker, publica
//  - Sino, le envia una solicitud al coordinador para obtener esos datos
//30faab00-2339-4e57-928a-b78cabb4af6c
function intentaPublicar(contenido,topico){
    //Si tengo la ubicacion del topico (broker) guardada, lo envio
    let mensajePub = {
        emisor: userId,
        mensaje: contenido
    } 
    if(topicoIpPuertoPub.hasOwnProperty(topico)){ 
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
        // ACA ESTA LA MALARIA!!!! ESTAMOS GUARDANDO LOS topicoIpPuertoPub
        //PERO NO ESTAMOS GUARDANDO LOS topicoIpPuertoSub!!!!
        //DESACOPLAR!!!!
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

                //Fer: Yo aca llamaria a otra funcion que sea, conectarseAPub
                procesarAltaTopicos(reply.resultados.datosBroker);
                //conseguir el mensaje que queriamos enviar
                let mensaje = pendingPublications[reply.idPeticion];
                let topico = pendingRequests[reply.idPeticion].topico; 
                publicaEnBroker(mensaje,topico);
                break;
            case globals.COD_ALTA_SUB: 
                //Fer: A este metodo le pondria conectarseASub
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

