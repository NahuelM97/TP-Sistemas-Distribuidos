const { Console } = require('console');
const { request } = require('http');
const zmq = require('../zeromq/node_modules/zeromq');
const { COD_ADD_TOPICO_BROKER, COD_ALTA_SUB, COD_PUB } = require('../Global/Globals');
const globals = require('../Global/Globals');

// socket to talk to clients
var repSocket = zmq.socket('rep');
var reqSocket = zmq.socket('req');


const ipCoordinador = `127.0.0.1`;
const puertoCoordinador = 1234;


//key: el topico
//value: la id del broker q lo tiene
//por ej: 'topico1':1
//el 1 es la posicion en el arreglo de brokers
let topicoIdBroker = {};

//key la id del broker
// value el ippuerto
let brokerIpPuerto = [
    { ip: '127.0.0.1', puertoPub: 3000, puertoSub: 3001, puertoRep: 3002 }, // "id" 0
    { ip: '127.0.0.1', puertoPub: 3003, puertoSub: 3004, puertoRep: 3005 }, // "id" 1
    { ip: '127.0.0.1', puertoPub: 3006, puertoSub: 3007, puertoRep: 3008 }  // "id" 2
];

//TEST BROKERS
// let brokerIpPuerto = [
//     { ip: '127.0.0.1', puertoPub: 3000, puertoSub: 3001, puertoRep: 3002 },
//     { ip: '127.0.0.1', puertoPub: 3003, puertoSub: 3004, puertoRep: 3005 }
//      // "id" 0
// ];

let contadorTopicosPorBroker = [0, 0, 0];

// Arreglo paralelo con la ocupacion por broker
//TEST BROKERS
// let contadorTopicosPorBroker = [0,0];

// Guarda las solicitudes pendientes
var pendingRequests = {}; //TODO hacer esto porque no esta hecho
// aca van las request en el orden en que llegaron tipo cola

// ---------------------------------------- Main ------------------------------------------
// Se inicia la escucha 
{
    obtieneBrokersIniciales();
}

function obtieneBrokersIniciales(){
    let reqSocketInit = zmq.socket('req');
    
    reqSocketInit.on('message', cbSocketInit);

    enviarAsignacionTopicoBroker(reqSocketInit, 'heartbeat', false);    

    reqSocket.on('message', cbAsignaTopicoBroker);
    repSocket.on('message', cbRespondeRequest);
}

function cbAsignaTopicoBroker(responseJSON) { //Cuando el broker responde la solicitud de creacion del topico
    let response = JSON.parse(responseJSON);
    // Respuesta exitosa
    if (response && response.exito) {
        if (pendingRequests.hasOwnProperty(response.idPeticion)) {
            let topico = pendingRequests[response.idPeticion].mensaje.topico;

            

            let idBroker = pendingRequests[response.idPeticion].mensaje.idBroker;
            topicoIdBroker[topico] = idBroker;
            contadorTopicosPorBroker[idBroker]++;
            //nadie lo tiene, se lo asignamos a alguien        
            
           
            switch (pendingRequests[response.idPeticion].mensaje.accion) {
                case globals.COD_PUB:
                    enviarBrokerSegunTopicoExistente(topico, isPub);
                    break;
                case globals.COD_ALTA_SUB:
                    enviarDatosAlta(response.idPeticion);
                    break;
                default:
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

function cbRespondeRequest(requestJSON) { //un cliente quiere saber donde estan 1 o 3 topicos
    // Paso a objeto la request
    let request = JSON.parse(requestJSON);

    // Se deriva a la funcion que procesa la solicitud según corresponda
    switch (request.accion) {
        case globals.COD_PUB:
            procesarSolicitudPublicacion(request, COD_PUB)
            break;
        case globals.COD_ALTA_SUB:
            procesarAltaCliente(request, COD_ALTA_SUB);
            break;
        default:
            console.error("ERROR 4503: codigo binario no binario llegad esperado invalido de operacion en la solicitud");
        //algo salio mal esto no tendria que estar aca.
    }
    console.log("Received request: [", request, "]");
}




// asignamos los topicos heartbeat y message/all antes de empezar a atender request
function cbSocketInit(responseJSON) {
    //Cuando el broker responde la solicitud de creacion del topico
    let response = JSON.parse(responseJSON); 
    
    
    if (response && response.exito) {
        let topico = pendingRequests[response.idPeticion].mensaje.topico;
        let idBrokerMenosTopicos = getIdBrokerMenosTopicos();
        topicoIdBroker[topico] = idBrokerMenosTopicos;
        contadorTopicosPorBroker[idBrokerMenosTopicos]++;
        if (globals.getCantKeys(topicoIdBroker) == 2) { // ya se asignaron heartbeat y message/all a un broker.
            
            console.log('BIND?');
            repSocket.bind(`tcp://${ipCoordinador}:${puertoCoordinador}`); // aca empieza a escuchar requests
            console.log(`Escuchando a Clientes en: ${puertoCoordinador}`);
        } else {
            let reqSocketInit = zmq.socket('req');
    
            reqSocketInit.on('message', cbSocketInit);
            enviarAsignacionTopicoBroker(reqSocketInit, 'message/all', false);
        }
    }
    else {
        // TODO: Mandar respuesta al cliente de que no hubo exito.
    }
}





function getIdBrokerMenosTopicos() {
    let minCantTopicos = Math.min(...contadorTopicosPorBroker);
    return contadorTopicosPorBroker.indexOf(minCantTopicos);
}

// Envia respuesta al cliente del broker, del topico solicitado porque ya estaba asignado a un broker
function enviarBrokerSegunTopicoExistente(topico,isPub){
    
   
    let respuesta = 
    {
        exito: true,
        accion: COD_ALTA_SUB,
        idPeticion:  request.idPeticion,
        resultados: { 
            datosBroker: [
                {
                    topico: request.topico,
                    ip:  brokerIpPuerto[topicoIdBroker[request.topico]].ip,
                    puerto: brokerIpPuerto[topicoIdBroker[request.topico]].puertoPub,
                }
            ],
        }
}

    repSocket.send(JSON.stringify(respuesta));
}

// le avisamos a un broker que tiene un topico nuevo asignado
function enviarAsignacionTopicoBroker(socket,topico, accionCliente){
    let mensaje = {
        idPeticion: globals.generateUUID(),
        accion: accionCliente,
        topico: topico
    }

    let idBrokerMin = getIdBrokerMenosTopicos();


    let broker = brokerIpPuerto[idBrokerMin];
    socket.connect(`tcp://${broker.ip}:${broker.puertoRep}`); 
    let pendingRequest = {
        mensaje: mensaje,
        idBroker: idBrokerMin,
    }
    pendingRequests[mensaje.idPeticion] = pendingRequest;
    socket.send(JSON.stringify(mensaje));
    
}
       


// TODO: Terminar
// Funciones que procesan las solictudes
function procesarSolicitudPublicacion(request, accionCliente){
    // Si se tiene ese topico asignado a un broker, se envía directamente
    if (topicoIdBroker.hasOwnProperty(topico)) {
        enviarBrokerSegunTopicoExistente(topico,isPub);
    } 
    // De caso contrario, es necesario dar de alta el nuevo topico
    else {
        enviarAsignacionTopicoBroker(reqSocket, topico,isPub, accionCliente);
    }
}

function enviarDatosAlta(idPeticion){
    let brokerUser = {
        topico: pendingRequests[idPeticion].mensaje.topico,
        ip:  brokerIpPuerto[pendingRequests[idPeticion].idBroker].ip,
        puerto: brokerIpPuerto[pendingRequests[idPeticion].idBroker].puertoPub,
    }
    
    //TODO abstraer
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

    resultados.datosBroker[2] = brokerUser;

    
    let respuesta = globals.generarRespuestaExitosa(COD_ALTA_SUB, request.idPeticion, resultados)
    repSocket.send(JSON.stringify(respuesta));
    
}

function procesarAltaCliente(request, accionCliente){
    //mandamos los 3 juntos xq una request admite una sola reply (es el protocolo de ZMQ)

    console.log('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' + request);
      
    
    console.log('ENVIANDO DATOS ALTA');
    //let socketNuevoCliente = zmq.socket('req');

    // socketNuevoCliente.on('message', cbAsignaTopicoBroker);
    enviarAsignacionTopicoBroker(reqSocket, request.topico, accionCliente);

}




