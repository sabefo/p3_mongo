/*
* CABECERA AQUI
*/

// {
// "_id" :ObjectId("583ef650323e9572e2813189"), "director" :"Lourdes Sacristan Bernal",
// "titulo" :"Ex delectus vel dicta delectus.", "lanzamiento" :1967,
// "pais" :["Myanmar", "Azerbaiyan" ]
// }

// {
// "_id" :"fernandonoguera", "nombre" :"Pedro", "apellido1" :"Cordero", "apellido2" :"Bustos", "sexo" :"M",
// "gustos" :[ "terror", "comedia", "tragedia" ],
// "email" :"hporcel@arregui-belmonte.com",
// "edad" :54,
// "password" :"c46526e4f34352111b9f98feaacf1338b59d8d15", "direccion" :{"cod_postal" :"73182",
//               "numero" :90,
// "puerta" :"C",
// "pais" :"Alemania",
// "piso" :3,
// "nombre_via" :"Camino de Laura Rebollo", "tipo_via" :"Avenida" }
// "visualizaciones" :[
// { "_id" :ObjectId("583ef650323e9572e2813189"), "titulo" :"Ex delectus vel dicta delectus.", "fecha" :"1995-04-06" },
// { "_id" :ObjectId("583ef651323e9572e2813dd5"),
// "titulo" :"Ipsam repudiandae dolorem libero voluptatibus.",
// "fecha" :"1978-12-27" } ],
// }

/* AGGREGATION PIPELINE */
// 1.- Listado de paıs-numero de pelıculas rodadas en el, ordenado por numero de pelıculas descendente y en caso de empate por nombre paıs ascendente
function agg1() {
db.peliculas
  .aggregate([
    { $unwind: "$pais" },
    { $group: { _id: "$pais", count: { $sum: 1 } } },
    { $sort: { count: -1, _id: 1 } }
  ])
}

// 2.- Listado de los 3 tipos de pelıcula mas populares entre los usuarios de los ’Emiratos Árabes Unidos’, ordenado de mayor a menor numero de usuarios que les gusta. En caso de empate a numero de usuarios, se usa el tipo de pelıcula de manera ascendente.
function agg2() {
db.usuarios
  .aggregate([
    { $match: { "direccion.pais": "Emiratos Árabes Unidos" } }
  ])
}

// 3.- Listado de paıs-(edad mınima, edad-máxima, edad media) teniendo en cuenta unicamente los usuarios mayores de edad, es decir, con mas de 17 años. Los paıses con menos de 3 usuarios mayores de edad no deben aparecer en el resultado.
function agg3() {
db.usuarios
  .aggregate([
    { $match: { "edad": { $gt: 17 } } },
    { $project: { _id: 0, edad: "$edad", pais: "$direccion.pais" } },
    { $group: { _id: "$pais", min: { $min: "$edad" }, max: { $max: "$edad" }, avg: { $avg: "$edad" }, count: { $sum: 1 } } },
    { $sort: { count: -1, _id: 1 } },
  ])
}

// 4.- Listado de tıtulo pelıcula-numero de visualizaciones de las 10 pelıculas mas vistas, ordenado por numero descencente de visualizaciones. En caso de empate, romper por tıtulo de pelıcula ascendente.
function agg4() {
db.usuarios
  .aggregate([
    { $unwind: "$visualizaciones" },
    { $project: { _id: 0, "visualizaciones.titulo": 1 } },
    { $group: { _id: "$visualizaciones.titulo", count: { $sum: 1 } } },
    { $sort: { count: -1, _id: 1 } },
    { $limit: 10 }
  ])
}




/* MAPREDUCE */  
  
// 1.-
function mr1() {
  /* */
}

// 2.-
function mr2() {
  /* */
}

// 3.-
function mr3() {
  /* */
}

// 4.- 
function mr4() {
  /* */
}

