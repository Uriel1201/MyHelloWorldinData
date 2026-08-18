import sys

def saludo(primer_argumento, segundo_argumento):
    print("hola " + primer_argumento + " hola " + segundo_argumento)

def main(argumentos):
    saludo(argumentos[1], argumentos[2])

if __name__ == "__main__":
    main(sys.argv)
