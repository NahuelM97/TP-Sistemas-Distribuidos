
const net = require('net');


let config = require('./configNTP.json');
console.log(config);


const port = config.port;


let server = net.createServer(function (socket) {
  socket.on('data', function (dataJSON) {
    // tiempo de arribo del cliente
    let data = JSON.parse(dataJSON);

    
    let T2 = new Date();

    // tiempo de envío del servidor
    data['t2'] = T2.toISOString();
    
    let T3 = new Date();
    data['t3'] = T3.toISOString();
    socket.write(data);
  });

});

server.listen(port);

