import matplotlib.pyplot as plt
import numpy as np

class NBayes:
    def __init__(self, f1, f2):
        self.data1 = f1
        self.data2 = f2
        self.odl = 0
        self.Pkt1 = []
        self.Pkt1x = []
        self.Pkt1y = []
        self.Pkt2 = []
        self.Pkt2x = []
        self.Pkt2y = []
        self.readFileCSV()

    def readInput(self):
        while True:
            try:
                self.iloscSasiadow = int(input("Podaj ilość punktów sąsiednich -> "))
                self.newPointX = float(input("Wpisz współrzędną X -> "))
                self.newPointY = float(input("Wpisz współrzędną Y -> "))
                break
            except ValueError:
                print("Niepoprawny typ zmiennej")

        self.a_posteriori()
        self.showPlot()

    def readFileCSV(self):
        with open(self.data1, "r") as read:
            lines = read.readlines()
            for i in lines:
                x, y = i.strip().split("|")
                self.Pkt1.append(((float(x.replace(",", "."))), float(y.replace(",", "."))))
                self.Pkt1x.append(float(x.replace(",", ".")))
                self.Pkt1y.append(float(y.replace(",", ".")))

        with open(self.data2, "r") as read:
            lines = read.readlines()
            for i in lines:
                x, y = i.strip().split("|")
                self.Pkt2.append(((float(x.replace(",", "."))), float(y.replace(",", "."))))
                self.Pkt2x.append(float(x.replace(",", ".")))
                self.Pkt2y.append(float(y.replace(",", ".")))

    def showPlot(self):
        plt.plot(self.newPointX, self.newPointY, "xy")
        plt.plot(self.Pkt1x, self.Pkt1y, ".r")
        plt.plot(self.Pkt2x, self.Pkt2y, ".b")
        kolo = plt.Circle((self.newPointX, self.newPointY), self.odl, fill = False, color='g')
        plt.gcf().gca().add_artist(kolo)
        plt.show()

    def a_priori(self):
        Pkt1_ile = len(self.Pkt1)
        Pkt2_ile = len(self.Pkt2)
        suma = Pkt1_ile + Pkt2_ile

        return Pkt1_ile / suma, Pkt2_ile / suma

    def odleglosc(self, pktX, pktY):
        return np.sqrt(((self.newPointX - pktX) ** 2) + ((self.newPointY - pktY) ** 2))

    def chanceToList(self):
        Pkt1_copy = self.Pkt1.copy()
        Pkt2_copy = self.Pkt2.copy()
        Pkt1_counter = 0
        Pkt2_counter = 0
        vector = []

        for i in Pkt1_copy:
            x = self.odleglosc(i[0], i[1])
            vector.append((x, "Pkt1"))

        for i in Pkt2_copy:
            x = self.odleglosc(i[0], i[1])
            vector.append((x, "Pkt2"))

        for i in range(self.iloscSasiadow):
            najblizsze = min(vector)
            if najblizsze[1] == "Pkt1":
                Pkt1_counter += 1
            else:
                Pkt2_counter += 1
            if i + 1 == self.iloscSasiadow:
                self.odl = najblizsze[0]
            vector.remove(najblizsze)

        return Pkt1_counter / len(self.Pkt1), Pkt2_counter / len(self.Pkt2)

    def a_posteriori(self):
        Pkt1_prawd, Pkt2_prawd = self.chanceToList() # prawd ze wgledu na ilosc sasiadujacych pkt do danego zbioru punnktow1 i punktow2
        pierwsze_a_priori, drugie_a_priori = self.a_priori() # prawd pkt1 do wszystkich pkt i prawd pkt2 do wszystkich

        if pierwsze_a_priori * Pkt1_prawd > drugie_a_priori * Pkt2_prawd:
            return self.Pkt1.append((self.newPointX, self.newPointY)), self.Pkt1x.append(self.newPointX), self.Pkt1y.append(self.newPointY)
        else:
            return self.Pkt2.append((self.newPointX, self.newPointY)), self.Pkt2x.append(self.newPointX), self.Pkt2y.append(self.newPointY)




if __name__ == '__main__':
    file1= "data1.csv"
    file2 = "data2.csv"
    nb = NBayes(file1, file2)

    ile_razy = int(input("Podaj ile razy chcesz wczytać punkty -> "))

    for i in range(ile_razy):
        nb.readInput()


