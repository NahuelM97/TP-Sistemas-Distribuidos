//TODO FIJARSE SI SE PUEDE TRARE DE UN ARCHIVO
let brokerIpPuerto = [
    { ip: '127.0.0.1', puertoPub: 3000, puertoSub: 3001, puertoRep: 3002 }, // "id" 0
    { ip: '127.0.0.1', puertoPub: 3003, puertoSub: 3004, puertoRep: 3005 }, // "id" 1
    { ip: '127.0.0.1', puertoPub: 3006, puertoSub: 3007, puertoRep: 3008 }  // "id" 2
];


const express = require('express')
const app = express()
const port = 9123


const globals = require('../Global/Globals');

//PP: Aca se pone a escuchar
app.listen(port, () => {
    console.log(`Servidor HTTP escuchando en http://localhost:${port}`)
})




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



app.get('/broker/:brokerId/topics', (req, res) => {

    let brokerId = req.params.brokerId;

    console.log(`recibi -> ${req.url}`)

    if (!isNaN(brokerId)) {
        //req con broker DESCOMENTAR 60 a 76 para uso real
        // let solicitudBroker ={ 
        //     idPeticion:  generateUUID(),
        //     accion: COD_GET_TOPICOS, 
        //     topico: null,

        // }

        // let reqSock = zmq.socket('req');
        // reqSock.connect(`tcp://${brokerIpPuerto[brokerId].ip}:${brokerIpPuerto[brokerId].puertoRep}`);

        // reqSock.on('message',function(reply){
        //     //La enviamos con el mismo formato que llega
        //     res.send(reply);
        // });

        // reqSock.send(solicitudBroker);
        res.header('Access-Control-Allow-Origin', '*');
        res.send(JSON.stringify({
            exito: true,
            resultados: {
                listaTopicos: ['topico1hola', 't2chau', 't3qonda']
            },
            // error: {
            //     codigo: 1,
            //     mensaje: 'error 1: paso la cosa mala numero 1'
            // }
        }));
    }
    else {
        let resp = {
            exito: false,
            error: {
                codigo: 1,
                mensaje: "Operacion inexistente"
            }
        }
        res.send('Error: broker.')

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
        //req con broker DESCOMENTAR DESDE ACA.
        // let solicitudBroker = {
        //     idPeticion: generateUUID(),
        //     accion: COD_GET_MENSAJES_COLA,
        //     topico: topic,
        // }

        // let reqSock = zmq.socket('req');
        // reqSock.connect(`tcp://${brokerIpPuerto[brokerId].ip}:${brokerIpPuerto[brokerId].puertoRep}`);

        // reqSock.on('message', function (reply) {
        //     //La enviamos con el mismo formato que llega
        //     res.send(reply);
        // });

        // reqSock.send(solicitudBroker); HASTA ACA.
        res.header('Access-Control-Allow-Origin', '*');//este y el de abajo son para debug
        res.send(JSON.stringify({
            exito: true,
            resultados: {
                mensajes: [{ emisor: 'pepe', mensaje: 'hola como va', fecha: '2020-12-12T11:22:33.000Z' },
                { emisor: 'lucas', mensaje: 'chau', fecha: '1150-12-12T11:22:33.000Z' },
                { emisor: 'daniel', mensaje: 'aaaaaaaaahhhhhhh', fecha: '1996-12-12T11:22:32.000Z' }]
            },
            // error: {
            //     codigo: 1,
            //     mensaje: 'error 1: paso la cosa mala numero 1'
            // }
        }));
    }
    else {
        let resp = {
            exito: false,
            error: {
                codigo: 1,
                mensaje: "Operacion inexistente"
            }
        }
        res.send('Error: broker.')

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
            idPeticion: generateUUID(),
            accion: COD_BORRAR_MENSAJES,
            topico: topic,
        }

        let reqSock = zmq.socket('req');
        reqSock.connect(`tcp://${brokerIpPuerto[brokerId].ip}:${brokerIpPuerto[brokerId].puertoRep}`);

        reqSock.on('message', function (reply) {
            //La enviamos con el mismo formato que llega
            res.send(reply);
        });

        reqSock.send(solicitudBroker);
    }
    else {
        let resp = {
            exito: false,
            error: {
                codigo: 1,
                mensaje: "Operacion inexistente"
            }
        }
        res.send('Error: broker.')

    }

})
