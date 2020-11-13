document.getElementById('botonTopicos').addEventListener('click', cbGetTopicos);
document.getElementById('botonMensajes').addEventListener('click', cbGetMensajes);

function cbGetTopicos() {
    //hacer un get al servidor
    let brokerId = document.getElementById('selectBrokerId').value;
    let url = `http://localhost:9123/broker/${brokerId}/topics`;

    var XHR = new XMLHttpRequest();
    XHR.addEventListener("load", cbMostrarTopicos);
    XHR.open("GET", url);
    XHR.send();

}

function cbMostrarTopicos() {
    console.log(this.responseText);
    let response = JSON.parse(this.responseText);

    document.getElementById('resultadoTopicos').classList.remove('escondido');

    let selectTopicos = document.getElementById("selectTopicoId");
    vaciarListaTopicos(selectTopicos);


    if (response.exito) {
        response.resultados.listaTopicos.forEach(topico => {
            let option = document.createElement("option");
            option.value = topico;
            option.text = topico;
            selectTopicos.add(option);
        });


    }
    else {
        console.log(`Error al solicitar los topicos Codigo: ${response.error.codigo} Descripcion: ${response.error.mensaje}`)
    }
    //agregar 1 elemento a la lista del select



}


function cbGetMensajes() {
    //hacer un get al servidor
    let brokerId = document.getElementById('selectBrokerId').value;
    let topicoId = document.getElementById('selectTopicoId').value;
    let url = `http://localhost:9123/broker/${brokerId}/topics/${topicoId}`;

    var XHR = new XMLHttpRequest();
    XHR.addEventListener("load", cbMostrarMensajes);
    XHR.open("GET", url);
    XHR.send();
}

function cbMostrarMensajes() {
    console.log(this.responseText); //quE ES ESTE THIS: parece ser el XHR

    let response = JSON.parse(this.responseText);

    document.getElementById('mostrarMensajesTopico').classList.remove('escondido');

    let ulMensajes = document.getElementById("ulResultadoMensajes");


    if (response.exito) {
        response.resultados.mensajes.forEach(mensaje => {

            let listElement = document.createElement("li");
            let textoLi = `Remitente: ${mensaje.emisor}, mensaje: ${mensaje.mensaje} y fecha: ${mensaje.fecha}`;
            listElement.appendChild(document.createTextNode(textoLi));
            ulMensajes.appendChild(listElement);
        });


    }
    else {
        console.log(`Error al solicitar los topicos Codigo: ${response.error.codigo} Descripcion: ${response.error.mensaje}`)
    }

}


//vacia el select de los topicos
function vaciarListaTopicos(selectTopicos) {
    for (let i = selectTopicos.options.length; i >= 0; i--)
        selectTopicos.remove(i);
}