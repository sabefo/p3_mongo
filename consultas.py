# -*- coding: utf-8 -*-
# Santiago Bermúdez y John Torres declaramos que esta solución
# es fruto exclusivamente de nuestro trabajo personal. No hemos sido
# ayudados por ninguna otra persona ni hemos obtenido la solución de
# fuentes externas, y tampoco hemos compartido nuestra solución con
# nadie. Declaramos además que no hemos realizado de manera desho-
# nesta ninguna otra actividad que pueda mejorar nuestros resultados
# ni perjudicar los resultados de los demás.

import json
import pymongo
from bson import ObjectId
import pprint

client = pymongo.MongoClient('localhost', 27017)
db = client['sgdi_pr3']
usuarios = db['usuarios']
peliculas = db['peliculas']

# 1. Fecha y título de las primeras 'n' peliculas vistas por el usuario 'user_id'
# usuario_peliculas( 'fernandonoguera', 3 )
def usuario_peliculas(user_id, n):
  pprint.pprint(list(usuarios.find({ '_id' : user_id }, { 'visualizaciones' : { '$slice' : n } })))

# 2. _id, nombre y apellidos de los primeros 'n' usuarios a los que les gusten
# varios tipos de película 'gustos' a la vez
# usuarios_gustos(  ['terror', 'comedia'], 5  )
def usuarios_gustos(gustos, n):
  pprint.pprint(list(usuarios.find({ 'gustos' : { '$all' : gustos } }).limit(5)))

# 3. _id de usuario de sexo 'sexo' y con una edad entre 'edad_min' e 'edad_max' incluidas
# usuario_sexo_edad('M', 50, 80)
def usuario_sexo_edad(sexo, edad_min, edad_max):
  pprint.pprint(list(usuarios.find({ 'edad' : { '$gt' : edad_min, '$lt' : edad_max } }, { 'sexo' : sexo })))

# 4. Nombre, apellido1 y apellido2 de los usuarios cuyos apellidos coinciden,
#    ordenado por edad ascendente
# usuarios_apellidos()
def usuarios_apellidos():
  pprint.pprint(list(usuarios.find({ '$where' : 'this.apellido1 == this.apellido2' })))

# 5.- Titulo de peliculas cuyo director empiece con un 'prefijo' dado
# pelicula_prefijo( 'Yol' )
def pelicula_prefijo(prefijo):
  pprint.pprint(list(peliculas.find({ 'director' : { '$regex' : '^' + prefijo } }, { 'titulo' : 1, 'director' : 1 })))

# 6.- _id de usuarios con exactamente 'n' gustos, ordenados por edad descendente
# usuarios_gustos_numero(6)
def usuarios_gustos_numero(n):
  pprint.pprint(list(usuarios.find({ 'gustos' : { '$size' : n } })))

# 7.- usuarios que vieron pelicula la pelicula 'id_pelicula' en un periodo
#     concreto 'inicio' - 'fin'
# usuarios_vieron_pelicula('583ef650323e9572e2812680', '2015-01-01', '2016-12-31')
def usuarios_vieron_pelicula(id_pelicula, inicio, fin):
  pprint.pprint(list(usuarios.find({ 'visualizaciones._id' : ObjectId(id_pelicula) }, { 'visualizaciones.fecha' : { '$gt' : inicio, '$lt' : fin } })))

usuario_peliculas('wgutiérrez', 3)
# usuarios_gustos(['terror', 'comedia'], 5)
# usuario_sexo_edad('M', 50, 80)
# usuarios_apellidos()
# pelicula_prefijo('Yol')
# usuarios_gustos_numero(6)
# usuarios_vieron_pelicula('583ef650323e9572e2812680', '2015-01-01', '2016-12-31')
