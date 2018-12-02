/*
* CABECERA AQUI
*/

// {
// "_id" :ObjectId("583ef650323e9572e2813189"), "director" :"Lourdes Sacristan Bernal",
// "titulo" :"Ex delectus vel dicta delectus.", "lanzamiento" :1967,
// "pais" :["Myanmar", "Azerbaiyan" ]
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
  /* */
}

// 3.- Listado de paıs-(edad mınima, edad-máxima, edad media) teniendo en cuenta unicamente los usuarios mayores de edad, es decir, con mas de 17 años. Los paıses con menos de 3 usuarios mayores de edad no deben aparecer en el resultado.
function agg3() {
  /* */
}

// 4.- Listado de tıtulo pelıcula-numero de visualizaciones de las 10 pelıculas mas vistas, ordenado por numero descencente de visualizaciones. En caso de empate, romper por tıtulo de pelıcula ascendente.
function agg4() {
  /* */
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

