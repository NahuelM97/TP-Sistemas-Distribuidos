const { Console } = require('console');
const { request } = require('http');
const zmq = require('../zeromq/node_modules/zeromq');
const { COD_ADD_TOPICO_BROKER, COD_ALTA_SUB, COD_PUB } = require('../Global/Globals');
const globals = require('../Global/Globals');



// DEBUG_MODE
const DEBUG_MODE = true;


// socket to talk to clients
var repSocket = zmq.socket('rep');

// se usa solo para la inicializacion de el topico heartbeat y el topico message/all. Posteriormente no se utiliza de nuevo
var reqSocketInit;

let config = require('./configCoordinador.json');

const ipCoordinador = config.ipCoordinador;
const puertoCoordinador = config.ipPuerto;


//key: el topico
//value: la id del broker q lo tiene
//por ej: 'topico1':1
//el 1 es la posicion en el arreglo de brokers
let topicoIdBroker = {};

//key la id del broker
// value el ippuerto

let brokerIpPuerto = config.brokerIpPuerto;

//TEST BROKERS
// let brokerIpPuerto = [
//     { ip: '127.0.0.1', puertoPub: 3000, puertoSub: 3001, puertoRep: 3002 },
//     { ip: '127.0.0.1', puertoPub: 3003, puertoSub: 3004, puertoRep: 3005 }
//      // "id" 0
// ];

let contadorTopicosPorBroker = new Array(brokerIpPuerto.length).fill(0); // inicializado abajo, tiene la forma [0, 0, 0] y cada numero representa cuantos topicos tiene cada broker

// Arreglo paralelo con la ocupacion por broker
//TEST BROKERS
// let contadorTopicosPorBroker = [0,0];

// Guarda las solicitudes pendientes
    //la estructura del pendingRequest es la siguiente:
    // - la key es la id del mensaje con la cual respondera el broker para confirmar la asignacion
    // - el value es un objeto compuesto por ejemplo: { requestDelCliente: request, idBroker: 123}
    // - el idBroker son los datos del broker que nos respondio
    // - el requestDelCliente es para saber con que ID responderle al cliente, y ademas para saber que puerto mandarle (pub o sub)
var pendingRequests = {}; 
// aca van las request en el orden en que llegaron tipo cola

// ---------------------------------------- Main ------------------------------------------
// Se inicia la escucha 
{
    
    
    repSocket.on('message', cbRespondeRequestDeCliente);

    obtieneBrokerInicial('heartbeat'); //engañosa, obtiene los 2 pero figura una sola
}

function obtieneBrokerInicial(topico){
    let requestAlBroker = {
        idPeticion: globals.generateUUID(),
        accion: COD_ADD_TOPICO_BROKER,
        topico: topico
    }

    let idBrokerMin = getIdBrokerMenosTopicos();

    let pendingRequest = {
        requestDelCliente: {topico: topico},
        idBroker: idBrokerMin,
    }
    //la estructura del pendingRequest es la siguiente:
    // - la key es la id del mensaje con la cual respondera el broker para confirmar la asignacion
    // - el idBroker son los datos del broker que nos respondio
    // - el requestDelCliente es para saber con que ID responderle al cliente, y ademas para saber que puerto mandarle (pub o sub)
    pendingRequests[requestAlBroker.idPeticion] = pendingRequest;

    let broker = brokerIpPuerto[idBrokerMin];

    reqSocketInit = zmq.socket('req'); //usamos uno por vez por que si no hay problemas
    reqSocketInit.on('message', cbSocketInit);
    reqSocketInit.connect(`tcp://${broker.ip}:${broker.puertoRep}`); 
    reqSocketInit.send(JSON.stringify(requestAlBroker));
   
    
    debugConsoleLog('MANDE');

    
}

// asignamos los topicos heartbeat y message/all antes de empezar a atender request
function cbSocketInit(responseJSON) {
    //Cuando el broker responde la solicitud de creacion del topico
    reqSocketInit.close();
    let response = JSON.parse(responseJSON); 
    
    
    if (response && response.exito) {
        let topico = pendingRequests[response.idPeticion].requestDelCliente.topico;
        let idBrokerMenosTopicos = getIdBrokerMenosTopicos();
        topicoIdBroker[topico] = idBrokerMenosTopicos;
        contadorTopicosPorBroker[idBrokerMenosTopicos]++;
        if (globals.getCantKeys(topicoIdBroker) == 2) { // ya se asignaron heartbeat y message/all a un broker.
            repSocket.bind(`tcp://${ipCoordinador}:${puertoCoordinador}`); // aca empieza a escuchar requests
            debugConsoleLog(`Escuchando a Clientes en: ${puertoCoordinador}`);
        } else {
            obtieneBrokerInicial('message/all');
        }
    }
    else {
        // TODO: Mandar respuesta al cliente de que no hubo exito.
    }
}



function cbBrokerAceptoTopico(responseJSON) { //Cuando el broker responde la solicitud de creacion del topico
    let response = JSON.parse(responseJSON);
    // Respuesta exitosa
    if (response && response.exito) {
        if (pendingRequests.hasOwnProperty(response.idPeticion)) {
            let requestDeCliente = pendingRequests[response.idPeticion].requestDelCliente;
            let topico = requestDeCliente.topico;
            let idBroker = pendingRequests[response.idPeticion].idBroker;
            topicoIdBroker[topico] = idBroker;
            contadorTopicosPorBroker[idBroker]++;
            //nadie lo tiene, se lo asignamos a alguien        
            
           
            switch (requestDeCliente.accion) {
                case globals.COD_PUB:
                    enviarBrokerSegunTopicoExistente(requestDeCliente);
                    break;
                case globals.COD_ALTA_SUB:
                    enviarTriplaTopicosSubACliente(response.idPeticion);
                    break;
                default:
                    debugConsoleLog('Error 2515: Codigo de operacion invalido');
            }
            delete pendingRequests[response.idPeticion]; //elimina porque la respuesta ya llego
        }
        else {
            // TODO: Llegó una rta de una request que no hice!
        }
    }
    else {
        // TODO: Mandar respuesta al cliente de que no hubo exito.
    }
}

function cbRespondeRequestDeCliente(requestJSON) { //un cliente quiere saber donde estan 1 o 3 topicos
    // Paso a objeto la request
    let request = JSON.parse(requestJSON);

    // Se deriva a la funcion que procesa la solicitud según corresponda
    switch (request.accion) {
        case globals.COD_PUB:
            procesarSolicitudPublicacion(request) // TODO 
            break;
        case globals.COD_ALTA_SUB:
            procesarSolicitudTopicoSub(request);
            break;
        default:
            console.error("ERROR 4503: codigo binario no binario llegad esperado invalido de operacion en la solicitud");
        //algo salio mal esto no tendria que estar aca.
    }
    debugConsoleLog("Received request: ");
    debugConsoleLog(request);
}






function getIdBrokerMenosTopicos() {
    let minCantTopicos = Math.min(...contadorTopicosPorBroker);
    return contadorTopicosPorBroker.indexOf(minCantTopicos);
}

// Envia respuesta al cliente del broker, del topico solicitado porque ya estaba asignado a un broker
// le dice a un cliente donde publicar
function enviarBrokerSegunTopicoExistente(solicitudDeCliente){
    let brokerEnviar = brokerIpPuerto[topicoIdBroker[solicitudDeCliente.topico]];
    let puertoEnviado = solicitudDeCliente.accion == COD_PUB? brokerEnviar.puertoSub : brokerEnviar.puertoPub;
    debugConsoleLog("Voy a enviar el puerto " + puertoEnviado);
    let resultados = { 
        datosBroker: [
            {
                topico: solicitudDeCliente.topico,
                ip:  brokerIpPuerto[topicoIdBroker[solicitudDeCliente.topico]].ip,
                puerto: puertoEnviado,
            }
        ],
    };
   
    let respuestaAlCliente = globals.generarRespuestaExitosa(solicitudDeCliente.accion,solicitudDeCliente.idPeticion,resultados);


    repSocket.send(JSON.stringify(respuestaAlCliente));
}

// le avisamos a un broker que tiene un topico nuevo asignado
//el codigo de operacion es siempre ADD_TOPICO_BROKER independientemente si me lo pidieron para pub o para sub.
function enviarAsignacionTopicoBroker(request){
    let requestAlBroker = {
        idPeticion: globals.generateUUID(),
        accion: COD_ADD_TOPICO_BROKER,
        topico: request.topico
    }

    let idBrokerMin = getIdBrokerMenosTopicos();

    let pendingRequest = {
        requestDelCliente: request,
        idBroker: idBrokerMin,
    }

    pendingRequests[requestAlBroker.idPeticion] = pendingRequest;

    


    let broker = brokerIpPuerto[idBrokerMin];

    let reqSocket = zmq.socket('req');
    reqSocket.on('message', cbBrokerAceptoTopico);

    reqSocket.connect(`tcp://${broker.ip}:${broker.puertoRep}`);
    reqSocket.send(JSON.stringify(requestAlBroker));
    
    
    debugConsoleLog('MANDE');
}
       


// TODO: HACER
// Funciones que procesan las solictudes
function procesarSolicitudPublicacion(request){
    // Si se tiene ese topico asignado a un broker, se envía directamente
    if (topicoIdBroker.hasOwnProperty(request.topico)) {
        enviarBrokerSegunTopicoExistente(request);
    } 
    // De caso contrario, es necesario dar de alta el nuevo topico
    else {
        enviarAsignacionTopicoBroker(request);
    }
}

// PP envia la ubicacion de los 3 topicos a los que tiene que estar suscripto todo cliente
// Se usa:
// - como alta cuando entra un cliente
// - cada vez que alguien se une a/crea un grupo
// - cuando un broker quiere suscribirse a heartbeat
function enviarTriplaTopicosSubACliente(idPeticion){
    let brokerTopicoPedido = {
        topico: pendingRequests[idPeticion].requestDelCliente.topico,
        ip:  brokerIpPuerto[pendingRequests[idPeticion].idBroker].ip,
        puerto: brokerIpPuerto[pendingRequests[idPeticion].idBroker].puertoPub,
    }
    
    let resultados = { 
            datosBroker: [ 
                {
                    topico: 'message/all',
                    ip:  brokerIpPuerto[topicoIdBroker['message/all']].ip,
                    puerto: brokerIpPuerto[topicoIdBroker['message/all']].puertoPub,
                },
                {
                    topico: 'heartbeat',
                    ip:  brokerIpPuerto[topicoIdBroker['heartbeat']].ip,
                    puerto: brokerIpPuerto[topicoIdBroker['heartbeat']].puertoPub,
                }                    
            ],
     }    

    resultados.datosBroker[2] = brokerTopicoPedido;

    
    let respuesta = globals.generarRespuestaExitosa(COD_ALTA_SUB, idPeticion, resultados)
    repSocket.send(JSON.stringify(respuesta));
    
}


//ante una solicitud devuelve los brokers que atienden los topicos message/all, heartbeat, y el solicitado en la request
function procesarSolicitudTopicoSub(request){
    //mandamos los 3 juntos xq una request admite una sola reply (es el protocolo de ZMQ)

    debugConsoleLog('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' + request);
    debugConsoleLog('ENVIANDO DATOS ALTA');

    if(topicoIdBroker.hasOwnProperty(request.topico)){ //caso el topico ya esta asignado, envio directo
        let pendingRequest = {
            requestDelCliente: request,
            idBroker: topicoIdBroker[request.topico],
        }

        pendingRequests[request.idPeticion] = pendingRequest;
        enviarTriplaTopicosSubACliente(request.idPeticion);
    }
    else
        enviarAsignacionTopicoBroker(request);//caso el topico no esta asignado, envio al broker primero

}




function debugConsoleLog(message) {
    if(DEBUG_MODE) {
        console.log(message);
    }
}


