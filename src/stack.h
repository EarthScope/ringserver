/***************************************************************************
 * stack.h
 *
 * Stack routines.
 *
 * The code base was originally written by Emin Marinian:
 * http://www.csua.berkeley.edu/~emin/index.html
 * http://alum.mit.edu/www/emin
 *
 * If STACK_DATA_TYPE is undefined then stack.h and stack.c will be
 * code for stacks of void *, if they are defined then they will be
 * stacks of the appropriate data type.
 *
 * Some clean-up and additions (StackShift, StackUnshift & StackSort):
 *   Chad Trabant IRIS Data Management Center
 *
 * modified: 2008.069
 ***************************************************************************/

#ifndef STACK_H
#define STACK_H 1

#ifdef __cplusplus
extern "C" {
#endif

#ifndef STACK_DATA_TYPE
#define STACK_DATA_TYPE void *
#endif

typedef struct StackNode {
  STACK_DATA_TYPE data;
  struct StackNode *prev;
  struct StackNode *next;
} StackNode;

typedef struct Stack {
  StackNode *top;
  StackNode *tail;
} Stack;

Stack *StackCreate ();
void   StackPush (Stack *theStack, STACK_DATA_TYPE newDataPtr);
void  *StackPop (Stack *theStack);
void   StackUnshift (Stack *theStack, STACK_DATA_TYPE newDataPtr);
void  *StackShift (Stack *theStack);
void   StackDestroy (Stack *theStack, void DestFunc(void *a));
int    StackNotEmpty (Stack *theStack);
Stack *StackJoin (Stack *stack1, Stack *stack2);
int    StackSort (Stack *theStack, int (*StackNodeCmp)(StackNode *a, StackNode *b));
  
#ifdef __cplusplus
}
#endif

#endif /* STACK_H */
