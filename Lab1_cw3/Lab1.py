import numpy as np
import time

lista1 = np.random.permutation(np.arange(5000))
lista2 = lista1.copy()
lista3 = lista1.copy()

def bubble_sort(lista):
    for a in range(len(lista)-1, 0, -1):
        for index in range(a):
            if lista[index] > lista[index + 1]:
                lista[index], lista[index + 1] = lista[index + 1], lista[index]
    return lista

def selection_sort(lista):
    for i in range(len(lista) - 1):
        minIndx = i
        minVal = lista[i]
        j = i + 1
        while j < len(lista):
            if minVal > lista[j]:
                minIndx = j
                minVal = lista[j]
            j += 1
        value = lista[i]
        lista[i] = lista[minIndx]
        lista[minIndx] = value
    return lista

def quick_sort(lista):
    def _quicksort(a_list, low, high):
        if low < high:
            p = partition(a_list, low, high)
            _quicksort(a_list, low, p)
            _quicksort(a_list, p + 1, high)

    def partition(a_list, low, high):
        pivot = a_list[low]
        while True:
            while a_list[low] < pivot:
                low += 1
            while a_list[high] > pivot:
                high -= 1
            if low >= high:
                return high
            a_list[low], a_list[high] = a_list[high], a_list[low]
            low += 1
            high -= 1

    _quicksort(lista, 0, len(lista) - 1)
    return lista


print("Lista1 -> ", lista1)
print("Lista2 -> ", lista2)
print("Lista3 -> ", lista3)
print("--------- Po sortowaniu -----------")

start = time.time()
bubble_sort(lista1)
end = time.time()

print("Bubble sort time ->", end - start, "s")
print("After buble sort", lista1)

start = time.time()
selection_sort(lista2)
end = time.time()

print("Selection sort time ->", end - start, "s")
print("After selection sort", lista2)

start = time.time()
quick_sort(lista3)
end = time.time()

print("Quick sort time ->", end - start, "s")
print("After quick sort", lista3)
