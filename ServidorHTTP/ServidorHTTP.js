const zmq = require('../zeromq/node_modules/zeromq');

let config = require('./configServidorHTTP.json');


let brokerIpPuerto = config.brokerIpPuerto;

let reqSock = zmq.socket('req');

const express = require('express')
const app = express()
const port = config.port;

const globals = require('../Global/Globals');

//PP: Aca se pone a escuchar
app.listen(port, () => {
    console.log(`Servidor HTTP escuchando en http://localhost:${port}`)
});








///////////////////////////////////////////////////


//GET Lista Topicos
//broker/${brokerId}/topics
//  "exito": boolean,
// “resultados”: {
//                      “listaTopicos”: [t1, …, tn] 
//                      },
//             “error”: {
//                          “codigo”: cod,
//                          “mensaje”: “description”
//                          }


//GET Lista Topicos
app.get('/broker/:brokerId/topics', (req, res) => {

    let brokerId = req.params.brokerId;

    console.log(`recibi -> ${req.url}`)

    if (!isNaN(brokerId)) {
        let solicitudBroker ={ 
            idPeticion:  globals.generateUUID(),
            accion: globals.COD_GET_TOPICOS, 
            topico: null,

        }

        
        reqSock.connect(`tcp://${brokerIpPuerto[brokerId].ip}:${brokerIpPuerto[brokerId].puertoRep}`);

        reqSock.removeAllListeners('message');
        reqSock.on('message', function(reply){ //TODO este on.('message' tiene que ejecutarse una sola vez o hacer muchos reqSock
            res.header('Access-Control-Allow-Origin', '*');
            res.send(reply);
        });
        
        
        

        reqSock.send(JSON.stringify(solicitudBroker));
    }
    else {
        let resp = {
            exito: false,
            error: {
                codigo: 1,
                mensaje: "Operacion inexistente"
            }
        }
        res.send(JSON.stringify(resp));

    }

})



//GET Lista Mensajes
// {
//     "exito": boolean,  
//     “resultados”: {
//                     “mensajes”: [m1, …, mn] 
//                  },
//     “error”: {
//                   “codigo”: cod,
//                   “mensaje”: “description”
//                   }
// }


app.get('/broker/:brokerId/topics/:topic', (req, res) => {

    let brokerId = req.params.brokerId;
    let topic = req.params.topic;

    console.log(`recibi -> ${req.url}`)


    //TODO fijarse que topic sea valido
    if (!isNaN(brokerId)) {
        let solicitudBroker = {
            idPeticion: globals.generateUUID(),
            accion: globals.COD_GET_MENSAJES_COLA,
            topico: topic,
        }


        reqSock.connect(`tcp://${brokerIpPuerto[brokerId].ip}:${brokerIpPuerto[brokerId].puertoRep}`);

        reqSock.removeAllListeners('message');
        reqSock.on('message', function(reply){ //TODO este on.('message' tiene que ejecutarse una sola vez o hacer muchos reqSock
            res.header('Access-Control-Allow-Origin', '*');
            res.send(reply);
        });

        reqSock.send(JSON.stringify(solicitudBroker)); 
        
    }
    else {
        let resp = {
            exito: false,
            error: {
                codigo: 1,
                mensaje: "Operacion inexistente"
            }
        }

        res.send(JSON.stringify(resp))

    }

})

//DELETE Mensajes en cola para ese topico en ese broker particular
// {
//     "exito": boolean,
//     “resultados”: {},
//     “error”: {
//                    “codigo”: cod,
//                    “mensaje”: “description”
//                    }
// } 
app.delete('/broker/:brokerId/topics/:topic', (req, res) => {
    let brokerId = req.params.brokerId;
    let topic = req.params.topic;

    console.log(`recibi -> ${req.url}`)


    //TODO fijarse que topic sea valido
    if (!isNaN(brokerId)) {
        //req con broker
        let solicitudBroker = {
            idPeticion: globals.generateUUID(),
            accion: globals.COD_BORRAR_MENSAJES,
            topico: topic,
        }

        reqSock.connect(`tcp://${brokerIpPuerto[brokerId].ip}:${brokerIpPuerto[brokerId].puertoRep}`);
        
        reqSock.removeAllListeners('message');
        reqSock.on('message', function(reply){ //TODO este on.('message' tiene que ejecutarse una sola vez o hacer muchos reqSock
            res.header('Access-Control-Allow-Origin', '*');
            res.send(reply);
        });

        reqSock.send(JSON.stringify(solicitudBroker));
    }
    else {
        let resp = {
            exito: false,
            error: {
                codigo: 1,
                mensaje: "Operacion inexistente"
            }
        }
        res.send(JSON.stringify(resp))

    }

})

