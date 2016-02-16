1.1	PLATAFORMA T-HOARDER

T-hoarder fue concebido como un medio para almacenar tuits sobre ciertos temas candentes en los que se pudieran analizar los distintos tipos de propagación que podían tener los mensajes. Actualmente la plataforma es una fuente de información elaborada, que puede ser consultada por aquellas personas que muestran un interés por los acontecimientos sociales en España. Ha transcendido su uso más allá de la investigación para dar a conocer la evolución de un conjunto de eventos de interés social como movilizaciones ciudadanas, opiniones sobre la crisis económica, escándalos políticos, corrupción, etc.

La plataforma T-hoarder almacena tuits por líneas temáticas y los procesa automáticamente en tres ejes: temporal, espacial y de relevancia. El eje temporal permite ver tanto la evolución en el tiempo de un conjunto de indicadores como la proporción de mensajes retransmitidos, los usuarios más mencionados o más activos, los hashtags más populares, las palabras más frecuentes, etc. El eje espacial ubica los tuits geográficamente y la relevancia muestra los mensajes más difundidos. Esta plataforma está dotada de una interfaz gráfica interactiva que facilita la navegación por estos tres ejes.

1.2	ARQUITECTURA T-HOARDER

T-hoarder tiene una arquitectura sencilla que evita la dependencia de otros paquetes software. Utiliza Unix como sistema operativo y está desarrollado en Python. Para almacenar información usa ficheros Unix en vez de bases de datos por los siguientes motivos:
•	Poder funcionar en entornos de desarrollo mínimos, como por ejemplo en una Raspberry PI.

•	Facilitar el traslado de los datos de un servidor a otro. Cuando las bases de datos son muy grandes, los respaldos/restauraciones son problemáticos.

•	No tener que definir a priori un modelo de datos desconociendo el tipo de información que se va a procesar.

•	Permitir mezclar las distintas colecciones de datos fácilmente.

•	No requerir el acceso a la información de forma aleatoria ya que el procesado de los datos es secuencial.


Su arquitectura se estructura en tres capas desacopladas para evitar que el tiempo de ejecución de una no interfiera sobre las otras. La comunicación entre estas capas es siempre mediante ficheros. La división funcional es la siguiente:

•	Capa 1: Recolección y almacenamiento de datos

•	Capa 2: Procesado de datos

•	Capa 3: Visualización


Los componentes están programados en Python, estando formado el nombre de cada programa por el nombre del componente y la extensión “py”, siendo fácilmente localizables en el repositorio.

1.2.1	CAPA 1: RECOGIDA Y ALMACENAMIENTO DE DATOS

T-hoarder dispone de una aplicación llamada Tweetdekc y un conjunto de usuarios en Twitter. Para poder acceder a las APIs de Twitter mediante OAuth es necesario crear las claves de acceso para la aplicación y para los usuarios. Estas claves se generan en el componente tweet_auth y quedan almacenadas en ficheros para que los componentes puedan autenticarse automáticamente ante las APIs.

T-hoarder utiliza la API Streaming frente a la API REST por los siguientes motivos:

1.	Porque es el método más adecuado para obtener información en tiempo real con la única limitación de los 50 TPS. La alternativa sería el uso del método GET /search/tweets de API REST, pero esto obligaría a hacer consultas periódicas para obtener los mensajes con la consiguiente complicación tanto en determinar la frecuencia de muestreo como evitar el límite de velocidad.
2.	Porque cuando se inicia un experimento, generalmente, se recogen los mensajes desde ese mismo momento durante un periodo largo de tiempo. En otro tipo de experimentos de duración corta (días) y en los que es necesario remontarse a una fecha anterior, caso de los trending topic (TT) no es necesario el uso de T-hoarder. Es más razonable obtener los tuits con el método GET /search/tweets de la API REST, con los que se podrán recuperar hasta siete días previos y obtener el dataset completo.

1.2.1.1	Captura de datos

Dentro de las opciones de búsqueda de la API Streaming, T-hoarder usa preferentemente las palabras clave y los usuarios. La geolocalización es información que carece de contexto temático y representa una muestra muy pequeña de los tuits (1,5% en España).

Los flujos de datos se obtienen mediante el componente tweet_streaming. Se pueden extraer varios flujos simultáneos ejecutando el componente en paralelo, cada uno con un usuario diferente.

La API suministra los tuits solicitados en formato JSON. Los datos se transforman a texto plano separados por tabulaciones en una línea por mensaje. Esto permite leer los mensajes que se van recogiendo con facilidad e importarlos en una hoja de cálculo, si se desea.

De toda la información recibida se seleccionan los datos que son de utilidad para analizar el contexto del tuit:

•	id_tweet: identificador del tuit. Es un número creciente que va asignando Twitter secuencialmente a cada mensaje.

•	timestamp: fecha y hora GMT de tuit.

•	@autor: nombre de usuario del autor del tuit.

•	texto: texto del tuit.

•	app: aplicación desde la que se ha publicado el tuit.

•	id_autor: identificador del autor. Es un número creciente que va asignando Twitter a los usuarios conforme se van dando de alta.

•	seguidores: número de seguidores en el momento de la publicación.

•	siguiendo: número de usuarios seguidos en el momento de la publicación.

•	mensajes: número de tuits publicados anteriormente.

•	localización: localización declarada en el perfil de usuario.

•	url: enlace si el tuit contiene una URL, en caso contrario se almacena el valor None.

•	geolocalización: coordenadas si el tuit está geolocalizado, en caso contrario se almacena None.

•	nombre: nombre proporcionado por el usuario.

•	bio: descripción del usuario.

•	url_media: URL si el tuit contiene información multimedia, en caso contrario se almacena el valor None.

•	tipo_media: tipo de información multimedia (foto, video,.), en caso de no existir, su valor es None.

•	lenguaje: idioma del tuit, si se ha podido detectar.

Algunos datos como localización, nombre y bio pueden contener saltos de línea o tabulaciones. Para evitar conflictos con los delimitadores se filtran las tabulaciones y los saltos de línea en estos datos.

Puede parecer poco eficiente almacenar información redundante como localización, nombre y bio pero se ha llegado a una solución de compromiso para que la información de los tuits esté auto-contenida, evitando la consulta de información exterior. Además, estos datos pueden cambiar con el tiempo, tanto que hasta existe la herramienta bioischanged  para conocer el historial de cambios de un usuario. Por este motivo, asociar esta información al momento en que se publicó el tuit es más riguroso.

1.2.1.2	Almacenamiento de los datos

Los datos se han organizado en una estructura de directorios predefinida y con una notación de prefijos y sufijos para facilitar la localización de la información almacenada. Desde una raíz inicial, denominada $STORE, se guardan los distintos flujos de datos de cada experimento y las claves de acceso a la API de Twitter.

Los flujos de datos de cada experimento se archivan en paquetes de ficheros. Los ficheros se van generando con el patrón de nombre streaming_experimento_x.txt, (x: 0,n). El primer fichero se numera desde cero y cuando alcanza el tamaño de 100MB se comprime y se crea un nuevo fichero con una numeración creciente. Al ser ficheros de texto, la compresión es muy eficiente y el tamaño de los datos se reduce a la tercera parte.

Debajo de $STORE se crea un directorio para cada experimento, cuyo nombre será datos_experimento_X. Dentro de ese directorio se encuentra el fichero para seleccionar los tuits, bien por palabras clave, por usuarios o por localizaciones. También se utiliza este directorio para almacenar los datos elaborados.

Las claves de acceso de la aplicación y de los usuarios se archivan en el directorio $STORE/keys.

1.2.2	CAPA 2: PROCESADO DE DATOS

Esta capa se ejecuta de forma independiente a la captura y almacenamiento para evitar pérdida de tuits. Se utiliza un cron para ejecutar periódicamente los algoritmos de esta capa.

Los experimentos de larga duración podrían generar colecciones de datos muy grandes que resultarían muy costosas de procesar. Sin embargo, gracias al método de almacenamiento de los flujos de datos en paquetes de ficheros delimitados por tamaño, es viable realizarlo por partes para luego integrar los resultados. Este método tiene las siguientes ventajas:

1.	Es factible procesar directamente los datos comprimidos con Python sin que se incremente el tiempo de ejecución.
2.	Al tener los ficheros un tamaño manejable, no hay problemas de escalabilidad de los algoritmos.
3.	Es posible procesar en paralelo los distintos paquetes de ficheros.
4.	No es necesario volver a procesar un paquete ya procesado, tan solo los datos nuevos desde la última iteración.

En esta fase también se dispone de una estructura de directorios predeterminada. En el directorio $RESOURCES se almacenan los distintos recursos necesarios para procesar los datos, como por ejemplo: tablas de nombres por género, geolocalización de localidades, diccionarios para clasificar tuits, etc.

En el directorio $WEB existirá un directorio para cada experimento en los que se almacenarán los datos elaborados para ser presentados en la interfaz gráfica web.

Para cada uno de los paquetes se realizan las siguientes operaciones:

•	Filtrar los falsos positivos

•	Extraer los indicadores 

•	Extraer relevancia

•	Extraer localización

•	Generar estado del paquete


1.2.2.1	Filtrado de falsos positivos

Algunas veces se capturan falsos positivos debido a que los términos de búsqueda contienen palabras ambiguas o las palabras de las expresiones no aparecen en el orden esperado. Por ejemplo, si recogemos tuits que contengan la expresión “metro de Madrid” la API nos proporcionará todos los tuits que contengan las palabras “metro” y “Madrid” independientemente del orden en que aparezcan. El resultado puede incluir tuits relacionados con el fútbol debido a que Metro es un canal de televisión que emite partidos del Real Madrid o del Atlético de Madrid. Los falsos positivos se detectan cuando al procesar los tuits aparecen mensajes que no son del contexto buscado. Esto implica que hay que descartar los mensajes no deseados y reprocesar el paquete. Para el filtrado se utiliza un fichero llamado filter.txt que contiene un conjunto de palabras o expresiones que no corresponden al contexto y que permite desechar los mensajes que las contengan.

El filtrado se realiza mediante el componente tweets_select_filter que permite seleccionar o excluir tuits que contengan algunos términos o expresiones o que hayan sido publicados por ciertos usuarios.

1.2.2.2	Extraer los indicadores

Para observar la evolución de los datos almacenados se van calculando una serie de indicadores para cada día que serán expuestos más tarde en el eje temporal. Estos indicadores proporcionan una idea de la participación y modo de publicación de los mensajes:

•	Número de tuits: cantidad de tuits recogidos.

•	Número de RTs: cantidad de tuits que son difundidos mediante el mecanismo de retransmisión.

•	Número de replies: cantidad de tuits que son respuestas a otro tuit.

•	Número de menciones: cantidad de tuits que contienen menciones.

•	Número de usuarios únicos: cantidad de usuarios diferentes que han tuiteado.

•	Número de usuarios nuevos: cantidad de usuarios que tuitean por primera vez ese día.

•	Top hashtags: para cada uno de los hashtags más mencionados, la cantidad de veces que aparece en los tuits.

•	Top palabras: para cada una de las palabras más frecuentes (no se tienen en cuenta las stop words), la cantidad de veces que aparece en los tuits.

•	Top usuarios mencionados: para cada uno de los usuarios más mencionados, la cantidad de veces que aparece en los tuits.

•	Top usuarios activos: para cada uno de los usuarios más activos, la cantidad de tuits que han publicado.


Los indicadores se extraen mediante el componente tweets_counter en dos pasos. El primer paso va descomponiendo cada tuit en entidades que se van acumulando de forma global. Una vez finalizado, el otro paso obtiene las entidades de cada tipo más frecuentes y se contabiliza su aparición día a día.

Primer paso:

Para cada tuit:

  Obtener autor y contabilizarlo
  Obtener menciones a usuarios y contabilizarlas
  Obtener el origen del tuit y contabilizarlo
  Obtener la localización declarada del autor y almacenarla
  Obtener las palabras del tuit que no sean stopwords y almacenarlas
  Obtener los hashtags del tuit y almacenarlos
  

Segundo paso:

Obtener el top de autores, menciones, orígenes del tuit, localizaciones, palabras y hashtags

Para cada día:

  Para cada tuit de ese día:
  Contabilizar el top de autores
  Contabilizar el top menciones a usuarios
  Contabilizar el top de los orígenes del tuit
  Contabilizar el top de localizaciones
  Contabilizar el top de palabras
  Contabilizar el top de hashtags
  
  
  
1.2.2.3	Extraer relevancia

En T-hoarder, la relevancia se mide por la difusión de los mensajes. Los mensajes se difunden porque captan la atención de otros usuarios que a su vez quieren darle visibilidad en su entorno. En Twitter, la propagación de mensajes se realiza mediante el mecanismo de RT. El RT es una convención creada en los inicios de Twitter por los usuarios que querían compartir un tuit con sus seguidores y se realizaba mediante la publicación del mensaje de otro usuario anteponiéndole las siglas RT y el nombre del autor original. A partir del 2009 Twitter incluyó un botón de RT que hacía lo mismo pero automáticamente, lo que facilitó mucho la propagación de mensajes. Generalmente se difunden los tuits con los que se está de acuerdo, por lo que se puede contabilizar como un voto positivo al mensaje (Conover et al., 2011).

T-hoarder descarta usar el dato del número de RTs que suministra la API de Twitter por ser un dato dinámico que varía con el tiempo y que además, en el momento de la captura del tuit, en tiempo real, su valor sería cero o un valor muy bajo. En su lugar, detecta la difusión de los tuits comparando la similitud de mensajes y teniendo en cuenta la estructura del RT. Por lo tanto detecta RTs automáticos y RTs manuales.

Se considera que un tuit es la retransmisión de otro cuando comienza por “RT @usuario” y el resto del texto coincide más de un 90% con algún tuit original anterior (Figura 4). La coincidencia puede no ser del 100% porque al retransmitirse mensajes de casi 140 caracteres se truncan, como es el caso del ejemplo siguiente.

La difusión de mensajes se calcula por día y para todo el período de captura de tuits. De esta forma se conoce lo más relevante de cada jornada y lo más destacado en global. Los datos que se almacenan de los tuits más difundidos son:

•	Identificador del tuit.

•	Fecha y hora del tuit.

•	Autor del tuit.

•	Texto del tuit.

•	Número de veces que se ha difundido.


Los mensajes más difundidos se obtienen con el componente tweets_talk. Cada tuit es comparado con un buffer de tuits previos analizados. Si se detecta que es una retransmisión de algunos de ellos se incrementa el contador de RTs, en caso contrario se almacena en el buffer como nuevo mensaje. Cada hora o cada 15.000 tuits se salvan los 2.000 tuits más difundidos del buffer y el resto se descarta. De esta manera se evita que el número de comparaciones con tuits no difundidos ralenticen el proceso. Se mantiene un búfer global y otro del día.

Para cada tuit:

  ¿Es RT de algún tuit del búfer global?
  
  Sí:
  
    Incrementar contador de RT del tuit del búfer global
    
  No:
  
   Almacenar el tuit en el búfer global
   
  ¿Es RT de algún tuit del búfer del día?
  
  Sí:
  
    Incrementar contador de RT del tuit del búfer del día
    
  No:
  
    Almacenar el tuit en el buffer del día
    
  ¿Hay cambio de hora o el búfer tiene más de 15000 tuits?
  
  Sí:
  
    Reducir el búfer global a los 2000 tuits con más RTs
    
    Reducir el búfer del día a los 2000 tuits con más RTs
    
  ¿Hay cambio de día?
  
  Sí:
  
    Almacenar el búfer del día
    
    Vaciar el búfer del día
    

1.2.2.4	Extraer localización

La ubicación de los tuits se puede conocer por dos caminos. El primero es por la localización declarada del perfil del usuario. Este dato puede no estar completado o contener el nombre de una ubicación ficticia por lo que no es posible ubicar todos los mensajes geográficamente. No obstante, es posible localizar un porcentaje elevado de tuits (entre el 60% - 70%). La segunda opción la proporcionan los tuits geolocalizados de los usuarios que tienen activada la geolocalización en Twitter. En este caso el porcentaje es mucho más pequeño (en España el 1,5%).

Para la localización por perfil de usuario se utiliza un fichero con los municipios de España  a los que se les ha calculado previamente sus coordenadas (longitud y latitud) y se han clasificado por autonomía y provincia. Con estos datos se pueden situar los tuits en un mapa y también se pueden agregar por provincia o por autonomías. Para la geolocalización simplemente se extraen las coordenadas del tuit.

Para cada día se almacenan por un lado los tuits localizados por perfil del usuario y por otro los tuits geolocalizados. En ambos casos se guardan los mismos datos:

•	Identificador del tuit

•	Fecha y hora del tuit

•	Autor

•	Texto del tuit

•	Coordenadas

Las localizaciones se obtienen con el componente tweets_location que analiza la localización declarada del autor del tuit y comprueba si coincide con algún municipio, provincia o autonomía de España.

Para cada tuit

  ¿Localización coincide con municipio?

  Sí:
  
    Agregar las coordenadas del municipio
    
  No:
  
    ¿Localización coincide con provincia?
    
    Sí:
    
      Agregar las coordenadas de la capital provincia
      
    No:
    
      ¿Localización coincide con la autonomía?
      
      Sí:
      
        Agregar las coordenadas de la capital de la autonomía
        
¿Está el tuit localizado?

Sí:

  Añadir al fichero de localizaciones
  
¿Está el tuit geolocalizado?

Sí:

  Añadir al fichero de geolocalizaciones
  
1.2.2.5	Generar estado del paquete

Cuando un paquete es procesado total o parcialmente se almacena, en un fichero denominado experimento_x_status.txt, una información de estado: 

•	Fecha inicial: fecha de tuit más antiguo.

•	Fecha final: fecha de tuit más reciente.

•	Estado: estado del proceso del paquete. Puede tomar los valores: semi-procesado, procesado.

•	Ultimo tuit procesado: identificador del último tuit procesado.

•	Longitud del paquete en el momento de procesarlo.

•	Número de tuits.

•	Tiempo de ejecución: tiempo de ejecución del paquete.


1.2.2.6	Integración de resultados

Los resultados están calculados por día, por lo que la integración es algo tan sencillo como la concatenación de resultados. Solo hay que tener en cuenta el efecto “borde” que se produce al dividir las colecciones de datos en paquetes de 100K. La partición puede dejar un día en diferentes paquetes.

En el caso de los tops (palabras, hashtags, usuarios mencionados y usuarios activos) se reduce del top 1000 que se almacena al top 10 para su visualización. Por este motivo hay que recalcular cuáles han sido los tops en el conjunto total de la colección de datos.

Los resultados se depositan en el directorio de intercambio $WEB para que el servidor web pueda acceder a ellos. El formato es texto plano con separadores y adaptado a las herramientas de visualización. Los datos se integran con el componente join_results.

Para cada paquete de datos

  Almacenar contadores de entidades
  
  Almacenar top de entidades (teniendo en cuenta que un día puede estar en dos paquetes diferentes)
  
  Almacenar RTs globales
  
  Almacenar RTs por día (teniendo en cuenta que un día puede estar en dos paquetes diferentes)
  
  Almacenar localizaciones
  
Generar resultado final de contadores de entidades

Generar top de entidades reduciendo el top de 1000 a 10

Generar RTs globales

Generar RTs por día

Generar localizaciones


1.2.3	CAPA 3: VISUALIZACIÓN

Para conocer la evolución de la información recuperada, T-hoarder dispone de unos paneles web que permiten visualizar los datos procesados. Estos paneles están construidos mediante una estructura que alberga <iframes>.

El <iframe> es un recurso HTML que permite anidar documentos HTML. Es muy utilizado para incrustar pequeñas piezas HTML con una función específica dentro de una página web. El diseño con <iframe> permite construir las webs como si fueran un puzle.

Estos paneles se apoyan en los siguientes recursos:

•	El framework boostrap  de HTML, estilos CSS y JavaScript que permiten dar una estructura, un estilo y una interactividad a los distintos elementos del panel.

•	La librería dygraphs  para las gráficas temporales, que tiene opciones avanzadas para favorecer la interactividad, permitiendo hacer zoom y llamar a funciones desde un punto de la gráfica para contextualizar la información.

•	Google Maps para la visualización de mapas (en un futuro en Cartodb ).

La creación de paneles se realiza con un conjunto de plantillas genéricas que se particularizan para cada caso mediante el comando make_panel.

1.2.3.1	Plantilla de la página principal

La plantilla home.html contiene la estructura del panel Web en la que se particulariza la descripción del experimento y el acceso a las distintas gráficas temporales o mapas. Para ello se sustituye el token “@experiment” por el nombre del experimento. La plantilla principal consta de cuatro partes:

1.	Barra de navegación desde la que se puede acceder a la información de la plataforma. Esta barra es común a todos los paneles.
2.	Descripción del experimento con las entidades que están siendo monitorizadas. Es un iframe con información textual.
3.	Opciones de menú para seleccionar distintas vistas de la información: usuarios, tipo de tuits, hashtags y palabras más frecuentes, usuarios más mencionados y más activos, localización y geolocalización de los mensajes, información del dataset y la ayuda.
4.	Gráficas interactivas con la información seleccionada en el menú. Es un iframe en el que se incrusta la página HTML que le corresponde a la opción del menú seleccionada. Existen dos tipos de gráficas, las temporales donde se muestra la evolución de los distintos indicadores y las geográficas que se representan mediante un mapa.


1.2.3.2	Plantilla para gráficas temporales

La plantilla grafica_panel_cgi.html contiene la estructura de la gráfica temporal que se particulariza para cada experimento. Para ello se sustituye el token “@experiment” por el nombre del dataset y el token “@data_file” por el nombre del fichero con los datos. Por lo tanto, esta plantilla se adapta a cada una de las gráficas temporales del experimento. 

Las gráficas temporales constan de dos partes interrelacionadas:
 
1.	La evolución de la entidad seleccionada, en la que se muestra gráficamente la variación de los distintos valores en el tiempo. Pasando el ratón por la gráfica se pueden ver los valores numéricos de los elementos de las leyendas. Para hacer zoom, se pulsa botón izquierdo del ratón y se arrastra. Para eliminar el zoom, se hace doble clic.
2.	Los tuits más relevantes de la entidad. Por defecto se muestran los más difundidos recientemente. Es posible ver los más propagados durante toda la duración del experimento pulsando en el botón “más difundidos”. Para descubrir los tuits más populares en un día concreto tan solo hay que hacer clic en la fecha de la gráfica de la izquierda. En todos los casos, los mensajes están paginados de cuatro en cuatro y se pueden consultar hasta un máximo de cuarenta.

1.2.3.3	Plantilla para localización de tuits

La plantilla grafica_location.html contiene un plano de Google Maps en el que se representa la frecuencia de tuits por áreas mediante un mapa de calor, resaltando con un código de color las zonas más densas de tuits (Figura 7). Se particulariza sustituyendo el token “@data_file” por el nombre del fichero con los datos. 

1.2.3.4	Plantilla para geolocalización de tuits

La plantilla grafica_geolocation.html contiene un mapa de Google Maps para situar los tuits geolocalizados mediante un puntero (Figura 8). Se particulariza sustituyendo el token “@data_file” por el nombre del fichero con los datos. Pasando el ratón por el puntero se puede leer el mensaje publicado en ese lugar.





