const { Console } = require('console');
const { request } = require('http');
const zmq = require('../zeromq/node_modules/zeromq');
const { COD_ADD_TOPICO_BROKER, COD_ALTA_SUB, COD_PUB } = require('../Global/Globals');
const globals = require('../Global/Globals');

// socket to talk to clients
var repSocket = zmq.socket('rep');
var reqSocket = zmq.socket('req');

// se usa solo para la inicializacion de el topico heartbeat y el topico message/all. Posteriormente no se utiliza de nuevo
var reqSocketInit = zmq.socket('req');

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
var pendingRequests = {}; //TODO hacer esto porque no esta hecho
// aca van las request en el orden en que llegaron tipo cola

// ---------------------------------------- Main ------------------------------------------
// Se inicia la escucha 
{
    reqSocketInit.on('message', cbSocketInit);
    reqSocket.on('message', cbBrokerAceptoTopico);
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
    reqSocketInit.connect(`tcp://${broker.ip}:${broker.puertoRep}`); 
    reqSocketInit.send(JSON.stringify(requestAlBroker));

    
    console.log('MANDE');

    
}

// asignamos los topicos heartbeat y message/all antes de empezar a atender request
function cbSocketInit(responseJSON) {
    //Cuando el broker responde la solicitud de creacion del topico
    let response = JSON.parse(responseJSON); 
    
    
    if (response && response.exito) {
        let topico = pendingRequests[response.idPeticion].requestDelCliente.topico;
        let idBrokerMenosTopicos = getIdBrokerMenosTopicos();
        topicoIdBroker[topico] = idBrokerMenosTopicos;
        contadorTopicosPorBroker[idBrokerMenosTopicos]++;
        if (globals.getCantKeys(topicoIdBroker) == 2) { // ya se asignaron heartbeat y message/all a un broker.
            repSocket.bind(`tcp://${ipCoordinador}:${puertoCoordinador}`); // aca empieza a escuchar requests
            console.log(`Escuchando a Clientes en: ${puertoCoordinador}`);
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
            let topico = pendingRequests[response.idPeticion].requestDelCliente.topico;
            let idBroker = pendingRequests[response.idPeticion].idBroker;
            topicoIdBroker[topico] = idBroker;
            contadorTopicosPorBroker[idBroker]++;
            //nadie lo tiene, se lo asignamos a alguien        
            
           
            switch (pendingRequests[response.idPeticion].requestDelCliente.accion) {
                case globals.COD_PUB:
                    enviarBrokerSegunTopicoExistente(topico, isPub);
                    break;
                case globals.COD_ALTA_SUB:
                    enviarTriplaTopicosSubACliente(response.idPeticion);
                    break;
                default:
                    console.log('Error 2515: Codigo de operacion invalido');
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
            procesarAltaCliente(request);
            break;
        default:
            console.error("ERROR 4503: codigo binario no binario llegad esperado invalido de operacion en la solicitud");
        //algo salio mal esto no tendria que estar aca.
    }
    console.log("Received request: [", request, "]");
}






function getIdBrokerMenosTopicos() {
    let minCantTopicos = Math.min(...contadorTopicosPorBroker);
    return contadorTopicosPorBroker.indexOf(minCantTopicos);
}

// Envia respuesta al cliente del broker, del topico solicitado porque ya estaba asignado a un broker
// le dice a un cliente donde publicar
function enviarBrokerSegunTopicoExistente(respuestaDelBroker){
    
    let resultados = { 
        datosBroker: [
            {
                topico: respuestaDelBroker.topico,
                ip:  brokerIpPuerto[topicoIdBroker[respuestaDelBroker.topico]].ip,
                puerto: brokerIpPuerto[topicoIdBroker[respuestaDelBroker.topico]].puertoPub,
            }
        ],
    };
   
    let respuestaAlCliente = globals.generarRespuestaExitosa(COD_ALTA_SUB,respuestaDelBroker.idPeticion,resultados);


    repSocket.send(JSON.stringify(respuestaAlCliente));
}

// le avisamos a un broker que tiene un topico nuevo asignado
//el codigo de operacion es siempre ADD_TOPICO_BROKER independientemente si me lo pidieron para pub o para sub.
function enviarAsignacionTopicoBroker(socket,request){
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
    socket.connect(`tcp://${broker.ip}:${broker.puertoRep}`); 
    socket.send(JSON.stringify(requestAlBroker));
    console.log('MANDE');
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
        enviarAsignacionTopicoBroker(reqSocket, topico,isPub);
    }
}

// PP envia la ubicacion de los 3 topicos a los que tiene que estar suscripto todo cliente
// Se usa:
// - como alta cuando entra un cliente
// - cada vez que alguien se une a/crea un grupo
// - cuando un broker quiere suscribirse a heartbeat
function enviarTriplaTopicosSubACliente(idPeticion){
    let brokerUser = {
        topico: pendingRequests[idPeticion].requestDelCliente.topico,
        ip:  brokerIpPuerto[pendingRequests[idPeticion].idBroker].ip,
        puerto: brokerIpPuerto[pendingRequests[idPeticion].idBroker].puertoPub,
    }
    
    //TODO abstraer
    let resultados = { 
            datosBroker: [ 
                {
                    topico: 'message/all', //TODO Poner en Global
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

    
    let respuesta = globals.generarRespuestaExitosa(COD_ALTA_SUB, idPeticion, resultados)
    repSocket.send(JSON.stringify(respuesta));
    
}

function procesarAltaCliente(request){
    //mandamos los 3 juntos xq una request admite una sola reply (es el protocolo de ZMQ)

    console.log('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' + request);
      
    
    console.log('ENVIANDO DATOS ALTA');
    //let socketNuevoCliente = zmq.socket('req');

    // socketNuevoCliente.on('message', cbAsignaTopicoBroker);
    enviarAsignacionTopicoBroker(reqSocket, request);

}




