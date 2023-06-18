from py2neo import Graph, Node, Relationship
import json

# Conexión a la base de datos de Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

def createDB():
  # Información de los juegos
  # Abrimos .json para leer los juegos
  with open('juegos.json', 'r') as file:
      # Escribe el contenido del JSON en el archivo
      juegos=json.load(file)

  # Crear nodos para Publishers, Desarrolladores, géneros, sistemas operativos y Graph Rend
  Publishers = {}
  Desarrolladores = {}
  genres = {}
  operating_systems = {}
  visibilities = {}
  player_modes = {}
  perspectives = {}

  for juego in juegos:
      # Ya sea una lista de caracteristicas o un solo nombre, verificamos que ese nombre no exista
      #Si existe, no se crea un nuevo nodo
      #Si no existe, se crea un nuevo nodo
      Publisher_Nombre = juego['Publisher']
      if Publisher_Nombre not in Publishers:
          Publishers[Publisher_Nombre] = Node('Publisher', Nombre=Publisher_Nombre)

      Desarrollador_Nombre = juego['Desarrollador']
      if Desarrollador_Nombre not in Desarrolladores:
          Desarrolladores[Desarrollador_Nombre] = Node('Desarrollador', Nombre=Desarrollador_Nombre)

      genre_Nombres = juego['Jugabilidad']
      for jugabilidad_Nombre in genre_Nombres:
          if jugabilidad_Nombre not in genres:
              genres[jugabilidad_Nombre] = Node("Jugabilidad", Nombre=jugabilidad_Nombre)

      os_Nombres = juego['Sistema Operativo']
      for os_Nombre in os_Nombres:
          if os_Nombre not in operating_systems:
              operating_systems[os_Nombre] = Node('Sistema Operativo', Nombre=os_Nombre)

      GraphRend_Nombre = juego['Graph Rend']
      if GraphRend_Nombre not in visibilities:
          visibilities[GraphRend_Nombre] = Node('Graph Rend', Nombre=GraphRend_Nombre)

      player_modes_list = juego['Player']
      for player_mode in player_modes_list:
          if player_mode not in player_modes:
              player_modes[player_mode] = Node("Player", Nombre=player_mode)

      perspective_Nombres = juego['Viewpoint']
      for Viewpoint_Nombre in perspective_Nombres:
          if Viewpoint_Nombre not in perspectives:
              perspectives[Viewpoint_Nombre] = Node("Viewpoint", Nombre=Viewpoint_Nombre)

  # Crear los nodos y relaciones en la base de datos
  for juego in juegos:
      Publisher_Nombre = juego['Publisher']
      Desarrollador_Nombre = juego['Desarrollador']
      genre_Nombres = juego['Jugabilidad']
      os_Nombres = juego['Sistema Operativo']
      GraphRend_Nombre = juego['Graph Rend']
      perspective_Nombres = juego['Viewpoint']
      player_modes_list = juego['Player']
      
      #Creamos nodo videojuego con todos sus atributos
      Videojuego = Node("Videojuego", Nombre=juego['Nombre'], Precio=juego['Precio'], Ventas=juego['Ventas'])
      Publisher = Publishers[Publisher_Nombre]
      Desarrollador = Desarrolladores[Desarrollador_Nombre]
      jugabilidad_list = [genres[genre_Nombre] for genre_Nombre in genre_Nombres]
      os_list = [operating_systems[os_Nombre] for os_Nombre in os_Nombres]
      GraphRend = visibilities[GraphRend_Nombre]
      viewpoints_list = [perspectives[perspective_Nombre] for perspective_Nombre in perspective_Nombres]
      player_modes_list = [player_modes[player_mode] for player_mode in player_modes_list]

      graph.create(Videojuego | Publisher | Desarrollador | Relationship(Videojuego, "Financiador", Publisher) |
                  Relationship(Videojuego, "Fabricante", Desarrollador))

      for jugabilidad_node in jugabilidad_list:
          graph.create(jugabilidad_node | Relationship(Videojuego, "Género", jugabilidad_node))

      for os_node in os_list:
          graph.create(os_node | Relationship(Videojuego, "Plataforma", os_node))
      
      #Como funciona create
      #Buscamos el nodo que queramos relacionar (GraphRend)
      #La barra se utiliza para combinar nodos y relaciones
      #Luego relationship es funcion de py2neo que crea la relacion entre videojuego y graphrend
      # "Presentacion" es el nombre de la relacion
      #al utilizar la barra (|), estamos combinando nodos y relaciones en una estructura única antes de crearlos
      # en el grafo. Al pasar los nodos y la relación como argumentos separados, se crean y se agregan al grafo de forma individual en llamadas separadas
      #  a graph.create().
      graph.create(GraphRend | Relationship(Videojuego, "Presentación", GraphRend))

      for viewponit_node in viewpoints_list:
          graph.create(viewponit_node | Relationship(Videojuego, 'Perspectiva', viewponit_node))

      for player_mode_node in player_modes_list:
          graph.create(player_mode_node | Relationship(Videojuego, "Dinámica", player_mode_node))

def deleteDB():
    graph.delete_all()

