#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>


#define N 20
#define d 4
// 4 processes (can change it on Makefile)

// source: https://www.geeksforgeeks.org/quick-sort/
void swap (int *a, int *b){
   int t = *a;
   *a = *b;
   *b = t;
}
int partition (int *arr, int low, int high){
   int pivot = arr[high];
   int i = (low -1);

   for (int j=low; j<= high-1; j++){
      if (arr[j] < pivot){
         i++;
         swap(&arr[i], &arr[j]);
      }
   }
   swap(&arr[i+1], &arr[high]);
   return (i+1);
}
void quickSort (int *arr, int low, int high){
   if (low<high){
      int pi = partition(arr, low, high);
      quickSort(arr, low, pi-1);
      quickSort(arr, pi+1, high);
   }
}

// implementations for 2d visualization
void printArray(int **array, int size, int dimensions){
   printf("{ ");
   for (int i=0; i<size; i++){
      printf("[");
      for (int j=0; j<dimensions; j++)
         printf(" %d ", array[i][j]);
      printf("] ");
   }
   printf("}\n");
}
void printPoint(int *point, int dimensions){
   printf("[");
   for (int i=0; i<dimensions; i++)
      printf(" %d ", point[i]);
   printf("]\n");   
}

int main( int argc, char** argv ) {
   srand(time(0));   // Use current time as seed for random generator
   /* Create dataset of N points, d dimensions each point */
   int points[N][d];
   for (int i=0; i<N; i++)
      for (int j=0; j<d; j++)
         points[i][j] = rand()%100;

   int useless_tag = 8;
   int index;
   int flag;
   int SelfTID, NumTasks, err;
   int *pivot = (int*) malloc(sizeof(int));
   int med_dist;
   MPI_Status mpistat;

   // Initialize MPI connection
   err = MPI_Init( &argc, &argv );
   if( err ) printf("Error=%i in MPI_Init\n",err);
   // Find Number of processes and SelfID of each process
   MPI_Comm_size( MPI_COMM_WORLD, &NumTasks );
   MPI_Comm_rank( MPI_COMM_WORLD, &SelfTID );

   if( SelfTID == 0 ) {
      // Print dataset
      printf("Dataset: { ");
      for (int i=0; i<N; i++){
         printf("[");
         for (int j=0; j<d; j++)
            printf(" %d ", points[i][j]);
         printf("] ");
      }
      printf("}\n");

      // dataset for each proccess
      int numOfItems = N/(NumTasks);
      int **mypoints = (int **)malloc(numOfItems*sizeof(int*));
      for (int i=0; i<numOfItems; i++)
         mypoints[i] = (int *)malloc(d*sizeof(int));
      for (int i=0; i<numOfItems; i++)
         for (int j=0; j<d; j++)
            mypoints[i][j] = points[(SelfTID)*numOfItems + i][j];

      // print my points
      printf("%d: My starting points\n", SelfTID);
      printArray(mypoints, numOfItems, d);

      // Random choise of pivot 
      pivot = mypoints[rand()%numOfItems];   

      // Broadcast pivot
      err = MPI_Bcast(pivot,d,MPI_INT,0,MPI_COMM_WORLD);
      if( err ) printf("Error=%i in MPI_Bcast\n",err);
      printf("%d: Broadcasted random pivot = ", SelfTID);
      printPoint(pivot, d);

      // find euclidean distance without root
      int *distances = (int*) malloc(numOfItems*sizeof(int));
      for (int i=0; i<numOfItems; i++)
         distances[i] = 0;
      printf("%d: Distances = [", SelfTID);
      for (int i=0; i<numOfItems; i++){
         for (int j=0; j<d; j++){
            distances[i] += (mypoints[i][j]-pivot[j])*(mypoints[i][j]-pivot[j]);
         }
         printf(" %d ", distances[i]);
      }
      printf("]\n");

      int k = 0;
      int *alldists = (int*) malloc(N*sizeof(int));
      for (int i=0; i<N/(NumTasks); i++){
         alldists[k] = distances[i];
         k++;
      }
      int *temp = (int*) malloc((N/(NumTasks))*sizeof(int));
      // Receive distances from other processes
      for (int i=1; i<NumTasks; i++){
         err = MPI_Recv(temp, N/(NumTasks), MPI_INT, i, MPI_ANY_TAG, MPI_COMM_WORLD, &mpistat);
         if ( err ) printf("Error=%i in MPI_Recv\n",err);
         for (int j=0; j<N/(NumTasks); j++){
            alldists[k] = temp[j];
            k++;
         }
      }

      printf("%d: Distances received = [", SelfTID);
      for (int i=0; i<N; i++)
         printf(" %d ", alldists[i]);
      printf("]\n");  
      quickSort(alldists, 0, N);
      printf("%d: Distances after quickSort = [", SelfTID);      
      for (int i=0; i<N; i++)
         printf(" %d ", alldists[i]);
      printf("]\n");
      med_dist = alldists[N/2];

      // Broadcast median
      err = MPI_Bcast(&med_dist,1,MPI_INT,0,MPI_COMM_WORLD);
      if( err ) printf("Error=%i in MPI_Bcast\n",err);
      printf("%d: Broadcasted median of distances = %d\n", SelfTID, med_dist);

      // Separate points find how many are smaller and bigger or equal to median
      int pr_info[] = {0, 0}; /* smaller and bigger or equal values than median */
      for (int i=0; i<numOfItems; i++){
         if (distances[i] < med_dist){
            pr_info[0]++;
         } else {
            pr_info[1]++;
         }     
      }
      //Send l and r to next ID
      err = MPI_Send(pr_info, 2, MPI_INT, SelfTID+1, useless_tag, MPI_COMM_WORLD);
      if( err ) printf("Error=%i in MPI_Send\n",err);
      printf("%d: send l = %d, r = %d to %d\n", SelfTID, pr_info[0], (N-1) - pr_info[1], SelfTID+1);

      int **newdata = (int **)malloc(numOfItems*sizeof(int*));
      for (int i=0; i<numOfItems; i++)
         newdata[i] = (int *)malloc(d*sizeof(int));
      int r = N-1;
      int l = 0;
      int *sendbuff = (int*)malloc((d+1)*sizeof(int));
      int local = 0;
      /* tag = 0 for small, tag = 1 for bigger or equal */
      for (int i=0; i<numOfItems; i++){
         if (distances[i] < med_dist){  /* Master always store local here */
            newdata[l%numOfItems] = mypoints[i];
            printf("%d: Store localy to position %d, new point = ", SelfTID, l%numOfItems);
            printPoint(newdata[l%numOfItems], d);
            l++;
            local++;
         } else {
            if (r/(NumTasks-1) != 0) {  /* Send if the point is not for this process */
               sendbuff = mypoints[i];
               sendbuff[d] = 1;
               err = MPI_Send(sendbuff, d+1, MPI_INT, r/(NumTasks-1), 1, MPI_COMM_WORLD);
               if( err ) printf("Error=%i in MPI_Send\n",err);
               printf("%d: send to %d,(last digit = 1) point = ", SelfTID, r/(NumTasks-1));
               printPoint(sendbuff, d+1);
               r--;
            } else {    /* Store local if needed */ /* never get's here */
               newdata[r%numOfItems] = mypoints[i];
               printf("%d: Stored localy to position %d, new point = ", SelfTID, r%numOfItems);
               printPoint(newdata[r%numOfItems], d);
               r--;
               local++;
            }
         }
      }
      printf("%d: finished sending and storing localy\n", SelfTID);

      MPI_Request mpireq[2];
      MPI_Status stats[2];
      int remains = numOfItems - local;
      printf("%d: %d remainders\n", SelfTID, remains);
      if (remains > 0) { /* This means that master has (N-r) empty newdata */
         int **temp2 = (int**) malloc(remains*sizeof(int*));
         for (int i=0; i<remains; i++){
            temp2[i] = (int*) malloc((d+1)*sizeof(int)); 
            err = MPI_Recv(temp2[i], d+1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &mpistat);
            if ( err ) printf("Error=%i in MPI_Recv\n",err);  
            printf("%d: received one\n", SelfTID);          
            if (temp2[i][d] == 0) {
               newdata[l%numOfItems] = temp2[i];
               printf("%d: Stored in position %d,(tag = 0) new point = ", SelfTID, l%numOfItems);
               printPoint(newdata[l%numOfItems], d);
               l++;
            }
            else if (temp2[i][d] == 1){
               newdata[r%numOfItems] = temp2[i];
               printf("%d: Stored in position %d,(tag = 1) new point = ", SelfTID, r%numOfItems);    
               printPoint(newdata[r%numOfItems], d);           
               r--;
            } 
            // MPI_Testany(2, mpireq, &index, &flag, &mpistat);
            // while (!flag) {
            //    MPI_Testany(2, mpireq, &index, &flag, &mpistat);
            // }
            // if(flag){      /* If some Irecv arrives */
            //    if (index == 0) {
            //       newdata[l%numOfItems] = templow[i];
            //       printf("%d: Stored in position %d,(tag = 0) new point = ", SelfTID, l%numOfItems);
            //       printPoint(newdata[l%numOfItems], d);
            //       l++;
            //    }
            //    else if (index == 1){
            //       newdata[b] = temphigh[i];
            //       printf("%d: Stored in position %d,(tag = 1) new point = ", SelfTID, b);    
            //       printPoint(newdata[b], d);           
            //       b--;
            //    }
            printf("%d: %d remainders\n", SelfTID, remains-(i+1));
         }
         //}  
         // for(int i=0; i<remains; i++){
         //    free(temphigh[i]);
         //    free(templow[i]);
         // }         
         // free(temphigh);
         // free(templow);  
      }
  

      // print my newpoints
      printf("%d: New points\n", SelfTID);
      printArray(newdata, numOfItems, d);

      // // free memory
      // for(int i=0; i<numOfItems; i++)
      //    free(mypoints[i]);
      // free(mypoints);
      // for(int i=0; i<numOfItems; i++)
      //    free(newdata[i]);
      // free(newdata);      
   } else {
      // dataset for each proccess
      int numOfItems = N/(NumTasks);
      int **mypoints = (int **)malloc(numOfItems*sizeof(int*));
      for (int i=0; i<numOfItems; i++)
         mypoints[i] = (int *)malloc(d*sizeof(int));
      for (int i=0; i<numOfItems; i++)
         for (int j=0; j<d; j++)
            mypoints[i][j] = points[(SelfTID)*numOfItems + i][j];

      // print my points
      printf("%d: My first points\n", SelfTID);
      printArray(mypoints, numOfItems, d);

      // Receive pivot
      err = MPI_Bcast(pivot,d,MPI_INT,0,MPI_COMM_WORLD);
      if ( err ) printf("Error=%i in MPI_Bcast\n",err);
      printf("%d: received pivot = ", SelfTID);
      printPoint(pivot, d);

      // find euclidean distance without root
      int *distances = (int*) malloc(numOfItems*sizeof(int));
      for (int i=0; i<numOfItems; i++)
         distances[i] = 0;
      printf("%d: Distances = [", SelfTID);
      for (int i=0; i<numOfItems; i++){
         for (int j=0; j<d; j++){
            distances[i] += (mypoints[i][j]-pivot[j])*(mypoints[i][j]-pivot[j]);
         }
         printf(" %d ", distances[i]);
      }
      printf("]\n");

      // Send distances
      err = MPI_Send(distances, numOfItems, MPI_INT, 0, useless_tag, MPI_COMM_WORLD);
      if( err ) printf("Error=%i in MPI_Send\n",err);
      // Receive median
      err = MPI_Bcast(&med_dist,1,MPI_INT,0,MPI_COMM_WORLD);
      if( err ) printf("Error=%i in MPI_Bcast\n",err);

      /* Serial */
      //Receive r and l from past ID
      int past_info[2];
      err = MPI_Recv(past_info, 2, MPI_INT, SelfTID-1, MPI_ANY_TAG, MPI_COMM_WORLD, &mpistat);
      if( err ) printf("Error=%i in MPI_Recv\n",err);
      int r = (N-1) - past_info[1];
      int l = past_info[0];
      printf("%d: received l = %d, r = %d from %d\n", SelfTID, l, r, SelfTID-1);
 
      // Separate points find how many are smaller and bigger or equal to median
      //int pr_info[] = {0, 0}; /* smaller and bigger or equal values than median */
      for (int i=0; i<numOfItems; i++){
         if (distances[i] < med_dist){
            past_info[0]++;
         } else {
            past_info[1]++;
         }     
      }
      if (SelfTID != NumTasks-1){ /* Everyone except last are sending their info */
         //Send smalls and biggs or equal that has passed to next ID
         err = MPI_Send(past_info, 2, MPI_INT, SelfTID+1, useless_tag, MPI_COMM_WORLD);
         if( err ) printf("Error=%i in MPI_Send\n",err);
         printf("%d: send l = %d, r = %d to %d\n", SelfTID, past_info[0], (N-1) - past_info[1], SelfTID+1);
         /* Until Here */
      }


      int **newdata = (int **)malloc(numOfItems*sizeof(int*));
      for (int i=0; i<numOfItems; i++)
         newdata[i] = (int *)malloc(d*sizeof(int)); 
      int local = 0;
      int *sendbuff = (int*)malloc((d+1)*sizeof(int));
      /* tag = 0 for small, tag = 1 for bigger or equal */
      for (int i=0; i<numOfItems; i++){
         if (distances[i] < med_dist){
            if (l/(NumTasks-1) != SelfTID){
               sendbuff = mypoints[i];
               sendbuff[d] = 0;
               err = MPI_Send(sendbuff, d+1, MPI_INT, l/(NumTasks-1), 0, MPI_COMM_WORLD);
               if( err ) printf("Error=%i in MPI_Send\n",err);
               printf("%d: send to %d,(last digit = 0) point = ", SelfTID, l/(NumTasks-1));
               printPoint(sendbuff, d+1);
               l++;
            } else {      /* Stored local if needed*/
               newdata[l%numOfItems] = mypoints[i];   
               printf("%d: Stored localy to position %d, new point = ", SelfTID, l%numOfItems);
               printPoint(newdata[l%numOfItems], d);
               l++;
               local++;
            }
         } else {
            if (r/(NumTasks-1) != SelfTID){
               sendbuff = mypoints[i];
               sendbuff[d] = 1;
               err = MPI_Send(sendbuff, d+1, MPI_INT, r/(NumTasks-1), 1, MPI_COMM_WORLD);
               if( err ) printf("Error=%i in MPI_Send\n",err);
               printf("%d: send to %d,(last digit = 1) point = ", SelfTID, r/(NumTasks-1));
               printPoint(sendbuff, d+1);
               r--;
            } else {     /* Stored local if needed*/
               newdata[r%numOfItems] = mypoints[i];
               printf("%d: Stored localy to position %d, new point = ", SelfTID, r%numOfItems);
               printPoint(newdata[r%numOfItems], d);
               r--;
               local++;
            }
         }
      }
      printf("%d: finished sending and storing localy\n", SelfTID);

      MPI_Request mpireq[2];
      MPI_Status stats[2];
      int remains = numOfItems - local;
      printf("%d: %d remainders\n", SelfTID, remains);
      if (remains > 0) { /* This means that master has (N-r) empty newdata */
         int **temp = (int**) malloc(remains*sizeof(int*));
         for (int i=0; i<remains; i++){
            temp[i] = (int*) malloc((d+1)*sizeof(int)); 
            err = MPI_Recv(temp[i], d+1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &mpistat);
            if ( err ) printf("Error=%i in MPI_Recv\n",err);  
            printf("%d: received one\n", SelfTID);
            if (temp[i][d] == 0) {
               newdata[l%numOfItems] = temp[i];
               printf("%d: Stored in position %d,(tag = 0) new point = ", SelfTID, l%numOfItems);
               printPoint(newdata[l%numOfItems], d);
               l++;
            }
            else if (temp[i][d] == 1){
               newdata[r%numOfItems] = temp[i];
               printf("%d: Stored in position %d,(tag = 1) new point = ", SelfTID, r%numOfItems);    
               printPoint(newdata[r%numOfItems], d);           
               r--;
            } 
             
            // MPI_Testany(2, mpireq, &index, &flag, &mpistat);
            // while (!flag) {
            //    MPI_Testany(2, mpireq, &index, &flag, &mpistat);
            // }
            // if(flag){      /* If some Irecv arrives */
            //    if (index == 0) {
            //       newdata[l%numOfItems] = templow[i];
            //       printf("%d: Stored in position %d,(tag = 0) new point = ", SelfTID, l%numOfItems);
            //       printPoint(newdata[l%numOfItems], d);
            //       l++;
            //    }
            //    else if (index == 1) {
            //       newdata[b] = temphigh[i];
            //       printf("%d: Stored in position %d,(tag = 1) new point = ", SelfTID, b);
            //       printPoint(newdata[b], d);
            //       b--;
            //    }
            printf("%d: %d remainders\n", SelfTID, remains-(i+1));
         }
         //}  
         // for(int i=0; i<remains; i++){
         //    free(temphigh[i]);
         //    free(templow[i]);
         // }         
         // free(temphigh);
         // free(templow);
      }
      
      // print my newpoints
      printf("%d: New points\n", SelfTID);
      printArray(newdata, numOfItems, d);

      // // free memory
      // for(int i=0; i<numOfItems; i++)
      //    free(mypoints[i]);
      // free(mypoints);
      // for(int i=0; i<numOfItems; i++)
      //    free(newdata[i]);
      // free(newdata);  
   }
   MPI_Finalize();
   return( 0 );
}
