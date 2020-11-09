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
let offsetHora;
let offsetAvg;

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

var arregloSockets = []; //almacena en formato clave valor nuestros brokers conectados
var clientesOnline = {};
let topicoIpPuertoPub = {};

{// PP
    initClient();
    initClientNTP();
}

function initClient() {
    reqSock.on("message", cbRespuestaCoordinador);
    reqSock.connect(`tcp://${coordinadorIP}:${coordinadorPuerto}`);
    var messageInicial = {
        accion: COD_ALTA_SUB,
        topico: `${MESSAGE_TOPIC_PREFIX}/${userId}`
    };

    console.log(`Enviando: ${JSON.stringify(messageInicial)}`);
    reqSock.send(JSON.stringify(messageInicial));



    var messageHeartbeatPub = {
        accion: COD_PUB,
        topico: HEARTBEAT_TOPIC_NAME
    }

    console.log(`Enviando: ${JSON.stringify(messageHeartbeatPub)}`);
    reqSock.send(JSON.stringify(messageHeartbeatPub));
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
      } else {
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
            emisor: userId,
            mensaje: contenido,
            fecha: getTimeNTP(),
        }
        pubSock.send(JSON.stringify(mensaje));
    } else {
        let mensaje = {
            accion: COD_PUB,
            topico: topico
        }
        reqSock.send(JSON.stringify(mensaje));
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

function algoritmoNTP(){

}

function asignarNuevoTopico(topico, ip, puerto){
    let ipPuerto = `${ip}:${puerto}`
    topicoIpPuertoPub[topico] = ipPuerto;
}
//El coordinador me envio Ip puerto broker (de cod 1 o cod 2)

//Con COD1: Es porque quiero publicar en un topico por primera vez
//Con COD2: Se ejecuta cuando se vuelve a conectar o se conecta por primera vez el cliente (triple msj)
function cbRespuestaCoordinador(replyJSON) { 
    
    let reply = JSON.parse(replyJSON);
    console.log("Received reply : [", reply, ']');// tiene el formato de un arreglo con 3 objetos que corresponden a 3 brokers

    //COD 1 o COD 2????
    //Ya que 
    //Respuesta:
    // { 
    // “topico”: “nombreTopico”,
    // “ip”: “ip” ,   
    // “puerto”: xx, 
    // }
    
    
    // switch (replyJSON.accion) {
    //     case COD_PUB:
    //         asignarNuevoTopico(broker.topico,broker.ip,broker.puerto);
    //         break;
    //     case COD_ALTA_SUB:
            reply.forEach((broker) => {
                let ipPuerto = `${broker.ip.toString()}:${broker.puerto.toString()}`;
                asignarNuevoTopico(broker.topico,broker.ip,broker.puerto);
                let socket = getSocketByURL(ipPuerto);
                socket.subscribe(broker.topico.toString());
                socket.on('message', cbMensaje);        
            })
        //     break;
        // default:
        //     console.error("CODIGO INVALIDO DE RESPUESTA EN CLIENTE");
        //     break;
  


}

function cbMensaje(topic, message) {
    if (topic = HEARTBEAT_TOPIC_NAME) { // lo revisamos en todos para mayor flexibilidad
        //actualizo el tiempo de conexion de alguien
        clientesOnline[message.emisor] = message.fecha;
    }
    else {
        console.log('Recibio topico:', topic.toString(), 'con mensaje:', message.toString());
    }

}



function getSocketByURL(ipPuerto) {
    if (!arregloSockets.hasOwnProperty(ipPuerto)) { //con este broker no hable nunca, lo agrego a mi lista de brokers
        let nuevoSocket = zmq.socket('sub');

        arregloSockets[ipPuerto.toString()] = nuevoSocket;
        nuevoSocket.connect(`tcp://${ipPuerto.toString()}`);
    }

    return arregloSockets[ipPuerto];
}






