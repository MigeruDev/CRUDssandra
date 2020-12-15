from cassandra.cluster import Cluster
import logging

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

#KEYSPACE = "BidGata"

def create_conn():
    cluster = Cluster(['127.0.0.1'], protocol_version=4)
    return cluster.connect()

def setting_up(session, keyspace, table):
    log.info("creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 
        'replication_factor': '1' }
        """ % keyspace)

    log.info("setting keyspace...")
    session.set_keyspace(keyspace)
    
    log.info("creating table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS %s.%s (
            ID text,
            name text,
            age text,
            ciclo text,
            PRIMARY KEY (ID)
        )
        """ % (keyspace, table))

def set_student(session):
     # execute SimpleStatement that inserts one student into the table
    print("_______________Ingresar nuevo estudiante________________\n")
    ID = input("ID: ")
    name = input("Nombre: ")
    age = input("Edad: ")
    ciclo = input("Ciclo: ")
    
    log.info("inserting new student...") 
    session.execute("INSERT INTO estudiantes (ID, name, age, ciclo) VALUES (%s,%s,%s,%s,%s)", 
                    [ID, name, age, ciclo])

def get_student(session):
    # execute SimpleStatement that retrieves one student from the table
    print("____________________Buscar estudiante___________________\n")
    ID = input("Ingrese el ID del estudiante: ")
    
    log.info("searching student...")
    result = session.execute("SELECT * FROM estudiantes WHERE ID = %s", 
                             [ID]).one()
    print (result)
    
def update_student(session):
    # execute SimpleStatement that updates the age of one student
    print("__________________Actualizar estudiante__________________\n")
    ID = input("Ingrese el ID del estudiante: ")
    age = input("Actualizar Edad: ")
    ciclo = input("Actualizar Ciclo: ")
    
    log.info("updating student...")
    session.execute("UPDATE estudiantes SET age =%s, ciclo =%s WHERE ID = %s", 
                    [age, ciclo, ID])

def delete_student(session):
    # execute SimpleStatement that deletes one student from the table
    print("__________________Dar de baja estudiante_________________\n")
    ID = input("INgrese el ID del estudiante: ")
    
    log.info("deleting student...")
    session.execute("DELETE FROM estudiantes WHERE ID = %s", 
                    [ID])

def show_all(session):
    # execute SimpleStatement that retrieves all students from the table
    print("______________________Base de Datos______________________\n")
    
    log.info("showing all students...")
    result = session.execute("SELECT * FROM estudiantes")
    print (result)

def main():
    session = create_conn()
    setting_up(session, "bidgata", "estudiantes")
    
    switch = {
        1: set_student,
        2: get_student,
        3: update_student,
        4: delete_student,
        5: show_all
    }

    option = 0

    while (option<6):
        print(".------------------------------------------------------.\n"+
            "|---------------| Bienvenido al Menu |-----------------|\n"+
            ".------------------------------------------------------.\n"+
            "1. Ingresar nuevo estudiante\n"+
            "2. Buscar estudiante\n"+
            "3. Dar de baja estudiante\n"+
            "4. Actualizar datos estudiante\n"+
            "5. Visualizar base de datos\n"+
            "6. Salir del sistema\n\n")

        try:
            option = int(input("Seleccione lo que desea hacer: "))
            func = switch.get(option, lambda:"Opcion no valida")
            func()
            print("\n")
        except:
            print("Ha ocurrido un error, vuelva a ingresar la opcion")
    
main()