# Chord DHT — Sistema distribuido de estado de usuarios

Implementación de una tabla de hash distribuida (DHT) basada en **Chord**, 
para el proyecto de aprobación de *Telecomunicaciones y Sistemas Distribuidos (UNRC)*:
mantener el estado (`conectado`/`desconectado`) de usuarios,
distribuido entre nodos, con costo de búsqueda logarítmico y tolerancia a
fallas sin pérdida de datos.

El documento completo de arquitectura, algoritmos, formato de mensajes,
supuestos, limitaciones y la demostración semi-formal de las propiedades
requeridas está en [`documentacion.pdf`](./documentacion.pdf).

## Requisitos

- Python 3.10+.

## Estructura del proyecto

| Archivo | Rol |
|---|---|
| `chord_hash.py` | Hashing consistente (SHA-1) e intervalos circulares |
| `node.py` | Lógica del protocolo Chord: `find_successor`, `join`, `stabilize`, `fix_fingers`, replicación, `check_predecessor` |
| `network.py` | Transporte sobre sockets TCP (`send(address, method, *args)`) |
| `chord_rpc.py` | Serialización JSON de los mensajes RPC |
| `server.py` | Levanta un nodo como proceso independiente, escuchando en un puerto |

## Ejecución
Cada proceso, al recibir eventos importantes (join, cambio de sucesor/predecesor,
detección de una falla, promoción de una réplica a dato primario), los va
imprimiendo por su cuenta. No hace falta consultar nada aparte para ver que
el anillo se está corrigiendo solo.

```bash
# Terminal 1: primer nodo, inicia su propio anillo
python3 server.py --port 9000

# Terminal 2: se une a través del primero
python3 server.py --port 9001 --join 127.0.0.1:9000

# Terminal 3, 4, 5...: cada uno se une a través de cualquier nodo ya existente
python3 server.py --port 9002 --join 127.0.0.1:9000
```

Para terminar o simular un crash de un nodo: `Ctrl+C` o `kill -9 <pid>` desde otra terminal.

### Interactuar con el anillo

La forma más directa es una llamada Python de una línea desde una nueva terminal:

```bash
python3 -c "
from network import Network
net = Network()
net.send('127.0.0.1:9000', 'write', 'juan_cruz', 'connected')
print(net.send('127.0.0.1:9001', 'read', 'juan_cruz'))
"
```

## Pruebas

Se realizó una demostración completa (formación de un anillo con siete nodos, escrituras y
lecturas de datos desde nodos arbitrarios, alta de un nodo en caliente y caída del nodo
dueño de una clave) que se encuentra documentada
en la sección **Demostración de ejecución** de
[`documentacion.pdf`](./documentacion.pdf).