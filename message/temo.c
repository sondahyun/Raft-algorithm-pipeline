#include <stdlib.h>
#include <stdio.h>

int arr2[5];

arr2: 주소값
arr + sizeof(int) * 0 == arr[0]
arr2[0]: 0번째 주소에 있는 값

arr** = arr2

int a = 1;
int* p = &a;

변수의 주소값은 *
1차원 배열의 주소값은 **
2차원 배열의 주소값은 ***

malloc: memory allocate

int** arr = (*int)malloc(sizeof(int)*4);

a->
a.arrat

int a = 17;

String(a)

String.format("%d", a);
Integer(a)

str(a)