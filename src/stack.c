/***************************************************************************
 * stack.c:
 *
 * Stack routines.
 *
 * The code base was originally written by Emin Marinian:
 * http://www.csua.berkeley.edu/~emin/index.html
 * http://alum.mit.edu/www/emin
 *
 * Further modifications were for cleanup, clarity and some
 * optimization.  Additions of StackNode.prev, doubly-linked Stacks,
 * StackShift(), StackUnshift() & StackSort by:
 *
 *   Chad Trabant, IRIS Data Management Center
 *
 * modified: 2008.069
 ***************************************************************************/

#include "stack.h"
#include <stdlib.h>

Stack *
StackCreate (void)
{
  Stack *newStack;

  newStack = (Stack *)malloc (sizeof (Stack));
  if (!newStack)
    return NULL;

  newStack->top = newStack->tail = NULL;
  return (newStack);
}

int
StackPush (Stack *theStack, STACK_DATA_TYPE newDataPtr)
{
  StackNode *newNode;

  if (!theStack)
    return -1;

  newNode = (StackNode *)malloc (sizeof (StackNode));
  if (!newNode)
    return -1;

  newNode->data = newDataPtr;

  if (!theStack->top)
  {
    newNode->prev  = NULL;
    newNode->next  = NULL;
    theStack->top  = newNode;
    theStack->tail = newNode;
  }
  else
  {
    newNode->prev       = NULL;
    newNode->next       = theStack->top;
    theStack->top->prev = newNode;
    theStack->top       = newNode;
  }

  return 0;
}

STACK_DATA_TYPE
StackPop (Stack *theStack)
{
  STACK_DATA_TYPE popData;
  StackNode *oldNode;

  if (!theStack || !theStack->top)
    return NULL;

  popData       = theStack->top->data;
  oldNode       = theStack->top;
  theStack->top = theStack->top->next;
  free (oldNode);

  if (!theStack->top)
    theStack->tail = NULL;
  else
    theStack->top->prev = NULL;

  return (popData);
}

int
StackUnshift (Stack *theStack, STACK_DATA_TYPE newDataPtr)
{
  StackNode *newNode;

  if (!theStack)
    return -1;

  newNode = (StackNode *)malloc (sizeof (StackNode));
  if (!newNode)
    return -1;

  newNode->data = newDataPtr;

  if (!theStack->tail)
  {
    newNode->prev  = NULL;
    newNode->next  = NULL;
    theStack->top  = newNode;
    theStack->tail = newNode;
  }
  else
  {
    newNode->prev        = theStack->tail;
    newNode->next        = NULL;
    theStack->tail->next = newNode;
    theStack->tail       = newNode;
  }

  return 0;
}

STACK_DATA_TYPE
StackShift (Stack *theStack)
{
  STACK_DATA_TYPE shiftData;
  StackNode *oldNode;

  if (!theStack || !theStack->tail)
    return NULL;

  shiftData      = theStack->tail->data;
  oldNode        = theStack->tail;
  theStack->tail = theStack->tail->prev;
  free (oldNode);

  if (!theStack->tail)
    theStack->top = NULL;
  else
    theStack->tail->next = NULL;

  return (shiftData);
}

void
StackDestroy (Stack *theStack, void DestFunc (void *a))
{
  StackNode *x;
  StackNode *y;

  if (theStack)
  {
    x = theStack->top;
    while (x)
    {
      y = x->next;
      if (DestFunc && x->data)
        DestFunc (x->data);
      free (x);
      x = y;
    }
    free (theStack);
  }
}

int
StackNotEmpty (Stack *theStack)
{
  return (theStack && theStack->top) ? 1 : 0;
}

Stack *
StackJoin (Stack *stack1, Stack *stack2)
{
  if (!stack1 || !stack2)
    return stack1 ? stack1 : stack2;

  if (!stack1->tail)
  {
    free (stack1);
    return (stack2);
  }
  else if (!stack2->top)
  {
    free (stack2);
    return (stack1);
  }
  else
  {
    stack1->tail->next = stack2->top;
    stack2->top->prev  = stack1->tail;
    stack1->tail       = stack2->tail;
    free (stack2);
    return (stack1);
  }
}

/***************************************************************************
 * SortStreamsStack:
 *
 * Sort a Stack using the mergesort alorthim.  StackNode entries are
 * compared using the supplied StackNodeCmp() function.  The mergesort
 * implementation was inspired by the listsort function published and
 * copyright 2001 by Simon Tatham.
 *
 * Return the number of merges completed on success and -1 on error.
 ***************************************************************************/
int
StackSort (Stack *theStack, int (*StackNodeCmp) (StackNode *a, StackNode *b))
{
  StackNode *p, *q, *e, *top, *tail;
  int nmerges, totalmerges;
  int insize, psize, qsize, i;

  if (!theStack || !StackNodeCmp)
    return -1;

  if (!theStack->top)
    return 0;

  top         = theStack->top;
  totalmerges = 0;
  insize      = 1;

  for (;;)
  {
    p    = top;
    top  = NULL;
    tail = NULL;

    nmerges = 0; /* count number of merges we do in this pass */

    while (p)
    {
      nmerges++; /* there exists a merge to be done */
      totalmerges++;

      /* step `insize' places along from p */
      q     = p;
      psize = 0;
      for (i = 0; i < insize; i++)
      {
        psize++;
        q = q->next;
        if (!q)
          break;
      }

      /* if q hasn't fallen off end, we have two lists to merge */
      qsize = insize;

      /* now we have two lists; merge them */
      while (psize > 0 || (qsize > 0 && q))
      {
        /* decide whether next element of merge comes from p or q */
        if (psize == 0)
        { /* p is empty; e must come from q. */
          e = q;
          q = q->next;
          qsize--;
        }
        else if (qsize == 0 || !q)
        { /* q is empty; e must come from p. */
          e = p;
          p = p->next;
          psize--;
        }
        else if (StackNodeCmp (p, q) <= 0)
        { /* First element of p is lower (or same), e must come from p. */
          e = p;
          p = p->next;
          psize--;
        }
        else
        { /* First element of q is lower; e must come from q. */
          e = q;
          q = q->next;
          qsize--;
        }

        /* add the next element to the merged list */
        if (tail)
          tail->next = e;
        else
          top = e;

        e->prev = tail;
        tail    = e;
      }

      /* now p has stepped `insize' places along, and q has too */
      p = q;
    }

    tail->next = NULL;

    /* If we have done only one merge, we're finished. */
    if (nmerges <= 1) /* allow for nmerges==0, the empty list case */
    {
      theStack->top  = top;
      theStack->tail = tail;

      return totalmerges;
    }

    /* Otherwise repeat, merging lists twice the size */
    insize *= 2;
  }
} /* End of StackSort() */
