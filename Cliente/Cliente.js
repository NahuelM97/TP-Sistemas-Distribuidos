var zmq = require('../zeromq/node_modules/zeromq')
    , pubSocket = zmq.socket('pub')
    , subSocket = zmq.socket('sub')
    , reqSocket = zmq.socket('req');

const globals = require('../Global/Globals');

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

// Guardamos todas las conexiones abiertas para evitar hacer connect a ippuerto que ya nos conectamos
let conexiones = [];

function init(myUsername) {// PP
    userId = myUsername;
    initClient();
    initClientNTP();
}

function initClient() {
    reqSocket.on('message', cbRespuestaCoordinador);             
    subSocket.on('message', cbProcesaMensajeRecibido);
    pubSocket.on('connectj', cbConnectPub);
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
    intervalHeartbeat = setInterval(cbIntervalHeartbeat, INTERVAL_ENVIO_HEARTBEAT);
    
}

function cbIntervalHeartbeat(){
    intentaPublicar("",HEARTBEAT_TOPIC_NAME);
}

function solicitarBrokerSubACoordinador(mensajeReq){
    pendingRequests[mensajeReq.idPeticion] = mensajeReq; 
    socketSendMessage(reqSocket, JSON.stringify(mensajeReq));
}

//Dado un mensaje, realiza el envio por subSocket
//  el mensaje es guardado en pendingRequest, para cuando el coordinador nos responda
//  podamos saber que queriamos mandar y que topico
function solicitarBrokerPubACoordinador(mensajeReq, mensajePub){
    pendingRequests[mensajeReq.idPeticion] = mensajeReq;  
    pendingPublications[mensajeReq.idPeticion] = mensajePub;
    socketSendMessage(reqSocket, JSON.stringify(mensajeReq));
}

function conectarseParaPub(ipPuerto){
    if(!conexiones.includes(ipPuerto)){
        pubSocket.connect(`tcp://${ipPuerto}`);
        conexiones.push(ipPuerto);
    }
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
        
        debugConsoleLog(`Se realiza envio: ${JSON.stringify(mensaje)}`);
        socket.send(mensaje);
    }
    else
    {
        console.error('Mensaje sin idPeticion no pudo ser enviado');
    }
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
function intentaPublicar(contenido,topico){
    //Si tengo la ubicacion del topico (broker) guardada, lo envio
    let mensajePub = {
        emisor: userId,
        mensaje: contenido,
        fecha: getTimeNTP()
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
//                                    </PUB>                                        //                                        
//////////////////////////////////////////////////////////////////////////////////////

//esta funcion devuelve el tiempo actual, ya corregido con el offset obtenido por NTP
function getTimeNTP(){
    let milisActual = new Date().getTime();

    // Add 3 days to the current date & time
    //   I'd suggest using the calculated static value instead of doing inline math
    //   I did it this way to simply show where the number came from
    milisActual += offsetHora;

    // create a new Date object, using the adjusted time
    return new Date(milisActual).toISOString();    
}

// TODO: Adaptar nuevo formato
function suscribirseABroker(brokers){
    brokers.forEach(broker => {
        let ipPuerto = `${broker.ip}:${broker.puerto}`;
        subSocket.connect(`tcp://${ipPuerto.toString()}`);  
        subSocket.subscribe(broker.topico);
        
        debugConsoleLog("Me suscribo a: " + broker.topico + " con IPPUERTO " + ipPuerto.toString());
        
    })
}
//El coordinador me envio Ip puerto broker (de cod 1 o cod 2)

//Con globals.COD1: Es porque quiero publicar en un topico por primera vez
//Con globals.COD2: Se ejecuta cuando se vuelve a conectar o se conecta por primera vez el cliente (triple msj)
function cbRespuestaCoordinador(replyJSON) { 

    
    debugConsoleLog('Recibi mensaje del coordinador');

    let reply = JSON.parse(replyJSON);
    debugConsoleLog("Received reply : [" + replyJSON + ']');// tiene el formato de un arreglo con 3 objetos que corresponden a 3 brokers

    if(reply && reply.exito) {
        switch (reply.accion) {
            case globals.COD_PUB:

                let broker = reply.resultados.datosBroker[0];
                let ipPuerto = `${broker.ip}:${broker.puerto}`;
                conectarseParaPub(ipPuerto);

                //!!!MUY FEO: DEPENDE DE LA RED, HARDWARE TODO!!!!
                //TODO
                setTimeout(() => {
                    
                    //conseguir el mensaje y topico que queriamos enviar
                    let mensaje = pendingPublications[reply.idPeticion];
                    let topico = pendingRequests[reply.idPeticion].topico; 
                    topicoIpPuertoPub[topico] = ipPuerto;
                    publicaEnBroker(mensaje,topico);

                }, 200);
                
                //====SI EL CONNECT ES BLOQUEANTE, PODEMOS PONER ESTO DE ABAJO===
                //====      SINO, PONERLO EN EL CALLBACK DEL CONNECT          ===
                
                
                //conseguir el mensaje y topico que queriamos enviar
                // let mensaje = pendingPublications[reply.idPeticion];
                // let topico = pendingRequests[reply.idPeticion].topico; 
                // topicoIpPuertoPub[topico] = ipPuerto;
                // publicaEnBroker(mensaje,topico);
                
               
                break;
            case globals.COD_ALTA_SUB: 
                //Fer: A este metodo le pondria conectarseASub
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


function cbConnectPub(){
    let mensaje = pendingPublications[reply.idPeticion];
    let topico = pendingRequests[reply.idPeticion].topico; 
    topicoIpPuertoPub[topico] = ipPuerto;
    publicaEnBroker(mensaje,topico);
}

// Llega un mensaje nuevo a un tópico al que estoy suscrito
function cbProcesaMensajeRecibido(topic, message) {
    if (topic == HEARTBEAT_TOPIC_NAME) { // lo revisamos en todos para mayor flexibilidad
        //actualizo el tiempo de conexion de alguien
        clientesUltimoHeartBeat[message.emisor] = message.fecha;
    }
    else {
        debugConsoleLog('Recibio mensaje de topico:', topic.toString(), ' - ', message.toString());
        
        let mensajeParsed = JSON.parse(message);
        mostrarMensajeInterfaz(getFormattedMessage(mensajeParsed));
    }
}

function getFormattedMessage(mensaje){
    let formattedFecha = getFormattedDate(mensaje.fecha);
    return `${formattedFecha} - ${mensaje.emisor}: ${mensaje.mensaje}`;
}

function getFormattedDate(date){
    let fecha = new Date(date);
    let formattedDay = fecha.getDay().toString().length == 1 ? "0" + fecha.getDay() : fecha.getDay(); // Si el día no empieza en 0 se lo agrega
    let formattedMonth = fecha.getMonth().toString().length == 1 ? "0" + fecha.getMonth() : fecha.getMonth(); // Si el mes no empieza en 0 se lo agrega
    let formattedHours = fecha.getHours().toString().length == 1 ? "0" + fecha.getHours() : fecha.getHours();
    let formattedMinutes = fecha.getMinutes().toString().length == 1 ? "0" + fecha.getMinutes() : fecha.getMinutes();
    let formattedFecha = formattedDay + '/' + formattedMonth + '/' + fecha.getFullYear() + ' ' + formattedHours + ':' + formattedMinutes;
    return formattedFecha;
}


// Precondicion: está registrado en clientesUltimoHeartbeat
function isUserOnline(user) {
    getTimeNTP() - clientesUltimoHeartBeat[user].getTime() <=  ONLINE_TOLERANCE;
}

function debugConsoleLog(message) {
    if(DEBUG_MODE) {
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
    intentaPublicar(contenido,MESSAGE_TOPIC_PREFIX+'/all');
    return 'Mensaje de difusión enviado';
}

function enviarMensajeGrupo(contenido,idGrupo) {
    intentaPublicar(contenido,GROUP_TOPIC_PREFIX+'/'+idGrupo);
    return 'Mensaje enviado al grupo ' + idGrupo;
}

function enviarMensajeUsuario(contenido,idUsuario) {
    if (!clientesUltimoHeartBeat.hasOwnProperty[idUsuario]) {
        return 'Se ha intentado enviar un mensaje a un usuario que no está registrado.';
    }
    if (!isUserOnline(idUsuario)) {
        return 'El usuario no está en línea.';
    }
    else {
        intentaPublicar(contenido,MESSAGE_TOPIC_PREFIX+'/'+idUsuario);
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
    solicitarBrokerSubACoordinador(suscripcionAGrupo);
}


module.exports = {
	// funciones
    enviarMensajeAll:enviarMensajeAll,
    enviarMensajeGrupo:enviarMensajeGrupo,
    enviarMensajeUsuario:enviarMensajeUsuario,
    suscripcionAGrupo:suscripcionAGrupo,
    init:init
}
