// broker.js
const zmq = require('../zeromq/node_modules/zeromq');

const globals = require('../Global/Globals');
const pub = require('../Publicador/pub');

let config = require('./configBroker.json');
let configClientNTP = require('../Global/configClientNTP.json');
const { COD_ERROR_TOPICO_INEXISTENTE } = require('../Global/Globals'); // ????




// DEBUG_MODE
const DEBUG_MODE = true;



// TODO - Sacar estos datos de un archivo JSON
const brokerIp = config.ip;
const BROKER_PUB_PORT = config.pubPort;
const BROKER_SUB_PORT = config.subPort;

const BROKER_REP_PORT = config.repPort;

const MAX_MENSAJES_COLA = config.maxMensajesCola;  
const MAX_DIF_TIEMPO_MENSAJE = 1000 * config.maxDifTiempoMensaje; // en milisegundos 300000


const INTERVALO_VERIF_EXP_MSJ = 1000 * config.intervaloVerifExpMsj; // cada cuanto tiempo se verifica el tiempo de expiracion de los mensajes en la cola de mensajes

const HEARTBEAT_TOPIC_NAME = 'heartbeat';

// definimos 2 sockets: el que escucha todos los mensajes entrantes (subSocket), y el que va a enviar los mensajes a destino (pubSocket)
const xsubSocket = zmq.socket('xsub'),  // el broker escucha a todos los publisher
	xpubSocket = zmq.socket('xpub') // el broker le manda a todos los subscriber

const repSocket = zmq.socket('rep'); //para contestar al servidor http y al coordinador

let colaMensajesPorTopico = {}; // key = topico, value = cola de mensajes ordenados por fecha ascendente
// cada cola de mensajes esta ordenada por fecha de envio del mensaje

//SERVER NTP
const net = require('net');

const portNTP = configClientNTP.portNTP;
const NTP_IP = configClientNTP.ipNTP;
const INTERVAL_NTP = 1000 * configClientNTP.intervalNTP; // seconds 1
const INTERVAL_PERIODO = 1000 * configClientNTP.intervalPeriodo;  //seconds 120
const TOLERANCIA_CLIENTE = 1000 * config.toleranciaCliente;
let i = configClientNTP.cantOffsetsNTP;
const total = configClientNTP.cantOffsetsNTP;
let offsetHora = 0;
let offsetAvg = 0;
let clientNTP;

const coordinadorIP = config.coordinadorIP;
const coordinadorPuerto = config.coordinadorPuerto;

//para verificar al momento de que se conecte alguien (para saber si es reconeccion)
var msClientesUltimoHeartBeat = {};



// PP

{
	// ambos sockets van a ser modo servidor, ya que los publicadores y suscriptores de nuestro sistema, los clientes, se conectaran al broker (y no al revés)
	
	initClientNTP();
	initXSubSocket();
	initXPubSocket();

	initRepSocket();
	validarTiempoExpiracionMensajes();
}


function validarTiempoExpiracionMensajes() {
	setInterval(function () {
		//se usa la key, para poder conocer la posicion de la cola en la lista
		//sino hay un tema de referencias feas

		// tomamos la cola de mensajes de cada topico
		Object.keys(colaMensajesPorTopico).forEach(key => {
			colaMensajesPorTopico[key] = colaMensajesPorTopico[key].filter(mensaje =>
				(new Date(getTimeNTP()).getTime() - new Date(mensaje.fecha).getTime() <= MAX_DIF_TIEMPO_MENSAJE)
				// solo dejamos mensajes que tengan menor diferencia de fecha con la actual a la permitida
			);
		});
	}, INTERVALO_VERIF_EXP_MSJ);
}


// inicializa el socket para recibir publicaciones y mandarlas a clientes a traves del subSocket
function initXPubSocket() {
	// se conectan los suscriptores esperando recibir mensajes
	xpubSocket.on('message', function (topic) {
		let topicString = topic.toString();
		let topicSinHeader = topicString.substring(1);
		if (colaMensajesPorTopico.hasOwnProperty(topicSinHeader)) { // el topico es valido QAOP
			xsubSocket.send(topic); // el broker se suscribe a ese topico para poder recibir mensajes de sus publicadores 
			
			if(topicString.charCodeAt(0) == 0){ // 0 -> desconexion | 1 -> conexion
				debugConsoleLog(`Se desconecto un suscriptor del topico: ${topicSinHeader}`);
			}
			else if(topicSinHeader.startsWith('message/') && topicSinHeader != 'message/all'){ //se suscribio un user a su propio topico
				debugConsoleLog(`Se conecto un suscriptor a topico: ${topicSinHeader}`);
				//le mandamos la cola del topico correspondiente
				debugConsoleLog('cola de mensajes por topico: ');
				debugConsoleLog(colaMensajesPorTopico);
				colaMensajesPorTopico[topicSinHeader].forEach((message) => {
					xpubSocket.send([topicSinHeader, JSON.stringify(message)]) // publicación directa, ya que este broker maneja ese topico
				});
				
				
			} //else{} es heartbeat,grupo o message/all
		}
		else { // le pidieron publicar en un topico que no administra
			debugConsoleLog(`Error 1239123: Suscripcion a un topico invalido: ${topicSinHeader}`);
		}
	})

	xpubSocket.bindSync(`tcp://${brokerIp}:${BROKER_PUB_PORT}`) //si un cliente quiere escuchar se suscribe a este
}

// inicializa el socket para mandar lo que se publica a los clientes
function initXSubSocket() {
	// se conectan los publicadores para que su mensaje sea redirigido a los suscriptores
	xsubSocket.on('message', function (topic, message) {// cada vez que se publica un mensaje a este broker se llama a esto

		if (colaMensajesPorTopico.hasOwnProperty(topic)) { // el topico es valido 
			debugConsoleLog(` LLEGO MSJ -> - topico: ${topic}, mensaje: ${message}`);
			procesaMensaje(topic, message);
		}
		else {
			debugConsoleLog(` DROPEANDO MSJ -> no hay topico valido - topico: ${topic}, mensaje: ${message}`);
		}
	});

	xsubSocket.bindSync(`tcp://${brokerIp}:${BROKER_SUB_PORT}`) //si un cliente quiere publicar se comunica con este
}

// si la cola de mensajes tiene espacio -> inserta el mensaje en la cola
// si la cola de mensajes no tiene espacio y el mensaje que llego es mas reciente que el mas viejo de la cola -> descarta el mensajes mas antiguo para que haya espacio
function procesaMensaje(topico, mensajeJSON) {
	let colaMensajes = colaMensajesPorTopico[topico];
	let mensaje = JSON.parse(mensajeJSON);
	let topicoStr = topico.toString();
	// Previene repetición cuando es un message/<usuario> (al enviar la cola message/all)
	if (!topicoStr.startsWith('message/') || //es heartbeat
		topicoStr == 'message/all' ||        //es message/all
		  (! colaMensajes.some(	mensajeCola => ((mensajeCola.emisor == mensaje.emisor) 
								&& (mensajeCola.fecha == mensaje.fecha))))) {
		
		// Si hay lugar, o si el nuevo tiene fecha más nueva que el más viejo (en cuyo caso lo desplazará)
		if (colaMensajes.length < MAX_MENSAJES_COLA || mensaje.fecha > colaMensajes[0].fecha) { // el mensaje cumple la condicion de la cola

			colaMensajes.push(mensaje);
			colaMensajes.sort(compararMensajesPorFecha); // ordenar por fecha

			if (colaMensajes.length > MAX_MENSAJES_COLA) {
				colaMensajes.shift(); // saca al mensaje mas antiguo
			}
			colaMensajesPorTopico[topico] = colaMensajes; // actualizamos la cola de mensajes de ese topico

			publicarMensajeValido(topico, mensajeJSON);
		}

	}
}


// el mensaje cumple con las condiciones de la cola
function publicarMensajeValido(topico, mensaje) {
	xpubSocket.send([topico, mensaje]) // distribucion del mensaje a suscriptores
	debugConsoleLog(` REDIRIGIENDO MSJ topico: ${topico}, mensaje: ${mensaje}`);
}


// inicializa el socket para responder peticiones del coordinador o el servidor HTTP
function initRepSocket() {
	repSocket.on('message', cbRespondeSolicitud);
	repSocket.bind(`tcp://${brokerIp}:${BROKER_REP_PORT}`);
}


// se ejecuta cuando me llega una request del coordinador o el server HTTP
function cbRespondeSolicitud(requestJSON) {
	let request = JSON.parse(requestJSON);


	let topico = request.topico;
	let mensaje;

	switch (request.accion) {
		//COORDINADOR
		case globals.COD_ADD_TOPICO_BROKER:
			if (!colaMensajesPorTopico.hasOwnProperty(topico)) { // le piden administrar a un topico que no administraba
				agregarColaMensajes(topico);

				if(topico == 'message/all'){ //suscribirme a heartbeat

					//solicito a coordinador el heartbeat para suscribirme
					initSocketsClienteReconectado();

					
					var messageInicial = {
						idPeticion: globals.generateUUID(),
						accion: globals.COD_ALTA_SUB,
						topico: HEARTBEAT_TOPIC_NAME
					};

					pub.solicitarBrokerSubACoordinador(messageInicial);
				}

				debugConsoleLog(`Asignaron el topico ${requestJSON}`);
			}

			mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, {});
			// los resultados van vacíos porque el coord ya sabe ip y puertos del broker

			repSocket.send(JSON.stringify(mensaje));

			
			break;

		//SERVIDOR HTTP
		case globals.COD_GET_TOPICOS:
			let topicos = globals.getKeys(colaMensajesPorTopico);

			let resultados = {
				listaTopicos: topicos // ['topico1','topico2',...]
			};

			mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, resultados);
			// los resultados van vacíos porque el coord ya sabe ip y puertos del broker

			repSocket.send(JSON.stringify(mensaje));

			debugConsoleLog(`Solicitaron los topicos`);
			break;

		case globals.COD_GET_MENSAJES_COLA:

			if (colaMensajesPorTopico.hasOwnProperty(topico)) {
				//manda cola de mensajes de ese topico
				let resultados = {
					mensajes: colaMensajesPorTopico[topico] // ['mensaje1','mensaje2',...]
				};
				mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, resultados);
			}
			else {
				//no maneja ese topico -> manda rta con codigo de error
				mensaje = globals.generarRespuestaNoExitosa(request.accion, request.idPeticion, globals.COD_ERROR_TOPICO_INEXISTENTE, `El topico ${topico} no es administrado por este broker`);
			}

			repSocket.send(JSON.stringify(mensaje));

			debugConsoleLog(`Solicitaron los mensajes del topico ${topico}`);
			break;

		case globals.COD_BORRAR_MENSAJES:
			if (colaMensajesPorTopico.hasOwnProperty(topico)) {
				//borra los mensajes de ese topico porque lo administraba
				borrarColaMensajesTopico(topico);
				let resultados = {};
				mensaje = globals.generarRespuestaExitosa(request.accion, request.idPeticion, resultados);
			}
			else {
				//no maneja ese topico -> no borra mensajes
				mensaje = globals.generarRespuestaNoExitosa(request.accion, request.idPeticion, globals.COD_ERROR_TOPICO_INEXISTENTE, `El topico ${topico} no es administrado por este broker`);

			}

			debugConsoleLog(`Solicitaron borrar la cola de mensajes del topico ${topico}`);
			break;

		default:
			mensaje = globals.generarRespuestaNoExitosa(request.accion, request.idPeticion, globals.COD_ERROR_OPERACION_INEXISTENTE, 'El codigo de operacion ingresado no se reconoce como un codigo valido');
			repSocket.send(JSON.stringify(mensaje));

			debugConsoleLog(`Error ${request.error.codigo}: ${request.error.mensaje}`);
			break;
	}

}

function initSocketsClienteReconectado(){
	pub.initReqSocket(coordinadorIP,coordinadorPuerto, suscribirseABroker);
	pub.initCbSubSocket(cbProcesaMensajeRecibido);
}

function suscribirseABroker(brokers) {
	let i = 0;
	let found = false;
    while(i < brokers.length && !found) {
		let broker = brokers[i];

		// Viene el tópico message/all y lo descartamos, sólo nos quedamos con el heartbeat
		if (broker.topico == HEARTBEAT_TOPIC_NAME) {
			let ipPuerto = `${broker.ip}:${broker.puerto}`;
			pub.conectarseParaSub(ipPuerto, broker.topico);

			debugConsoleLog("Me suscribo al heartbeat: " + broker.topico + " que está en IPPUERTO " + ipPuerto.toString());
			found = true;
		}
		i++;
    }
}

function cbProcesaMensajeRecibido(topic, messageJSON){ //te llegan heartbeats
	let message = JSON.parse(messageJSON);
	if (topic == HEARTBEAT_TOPIC_NAME) { // lo revisamos en todos para mayor flexibilidad
		//actualizo el tiempo de conexion de alguien
		if(!msClientesUltimoHeartBeat.hasOwnProperty(message.emisor)){ //es la primera vez que se conecta (enviarle solo message/all)
			msClientesUltimoHeartBeat[message.emisor] = new Date(message.fecha).getTime();
			
			colaMensajesPorTopico['message/all'].forEach((mensajeDeCola)=>{
				intentaPublicar(mensajeDeCola, 'message/'+message.emisor);
			});
		} else {
			let msHeartbeatAnterior =  msClientesUltimoHeartBeat[message.emisor];
			let msHeartbeatNuevo = new Date(message.fecha).getTime();
			msClientesUltimoHeartBeat[message.emisor] = msHeartbeatNuevo;
			if(msHeartbeatNuevo - msHeartbeatAnterior > TOLERANCIA_CLIENTE){ //estaba desconectado (se volvio a conectar)
				//mandar cola message all

				colaMensajesPorTopico['message/all'].forEach((mensajeDeCola)=>{
					intentaPublicar(mensajeDeCola, 'message/'+message.emisor);
				});

			}
		}
		debugConsoleLog(`Me llego un heartbeat  con fecha ${message.fecha} y de ${message.emisor}`)
	} else { console.error("No puede llegar mensaje de un topico distinto de heartbeat, solo me suscribi a el")}
}







//////////////////////////////////////////////////////////////////////////////////////
//                                      PUB                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////

// Trata de enviar un mensaje en un topico (broker).
//  - Si tiene el ip:puerto almacenado del broker, publica
//  - Sino, le envia una solicitud al coordinador para obtener esos datos
//30faab00-2339-4e57-928a-b78cabb4af6c
function intentaPublicar(mensaje, topico) {
	pub.intentaPublicarMensajeDeCola(mensaje,topico);
}


//////////////////////////////////////////////////////////////////////////////////////
//                                    </PUB>                                        //                                        
//////////////////////////////////////////////////////////////////////////////////////

// dado un topico crea su cola de mensajes
function agregarColaMensajes(topico) {
	colaMensajesPorTopico[topico] = [];
}

// dado un topico borra el contenido de su cola de mensajes
function borrarColaMensajesTopico(topico) {
	colaMensajesPorTopico[topico] = [];
}


function compararMensajesPorFecha(mensaje1, mensaje2) {
	if (mensaje1.fecha < mensaje2.fecha) {
		return -1;
	}
	if (mensaje1.fecha > mensaje2.fecha) {
		return 1;
	}
	// a debe ser igual b
	return 0;
}

//////////////////////////////////////////////////////////////////////////////////////
//                                CLIENT (BROKER) NTP                               //                                        
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


//sector cerrar bien las conexiones TCP

process.on('SIGHUP', function () {
	endClientNTP();

});

process.on('SIGINT', function () {
	endClientNTP();
});

async function endClientNTP() {
	await clientNTP.end();
	clientNTP.on('end', () => process.exit());
}


//////////////////////////////////////////////////////////////////////////////////////
//                                      /NTP                                         //                                        
//////////////////////////////////////////////////////////////////////////////////////




function debugConsoleLog(message) {
	if (DEBUG_MODE) {
		console.log(message);
	}
}
