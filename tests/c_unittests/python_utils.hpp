#ifndef __python_utils_hpp__
#define __python_utils_hpp__ 1
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/// From chapter 14.1. C++ RAII Wrappers Around PyObject*
///  of https://pythonextensionpatterns.readthedocs.io/en/latest/cpp_and_cpython.html

#include <Python.h>

/** General wrapper around a PyObject*.
* This decrements the reference count on destruction.
*/
class DecRefDtor {
  public:
   DecRefDtor(PyObject *ref) : m_ref { ref } {}
   Py_ssize_t ref_count() const { return m_ref ? Py_REFCNT(m_ref) : 0; }
   // Allow setting of the (optional) argument with PyArg_ParseTupleAndKeywords
   PyObject **operator&() {
       Py_XDECREF(m_ref);
       m_ref = NULL;
       return &m_ref;
   }
   // Access the argument
   operator PyObject*() const { return m_ref; }
   // Test if constructed successfully from the new reference.
   explicit operator bool() { return m_ref != NULL; }
   ~DecRefDtor() { Py_XDECREF(m_ref); }
  protected:
   PyObject *m_ref;
};

/** Wrapper around a PyObject* that is a borrowed reference.
 * This increments the reference count on construction and
 * decrements the reference count on destruction.
 *
 * This can be used with borrowed references as follows:

void function(PyObject *obj) {
    BorrowedRef(obj); // Increment reference here.
    // ...
} // Decrement reference here.

 */
class BorrowedRef : public DecRefDtor {
   public:
    BorrowedRef(PyObject *borrowed_ref) : DecRefDtor(borrowed_ref) {
        Py_XINCREF(m_ref);
    }
};

/** Wrapper around a PyObject* that is a new reference.
 * This owns the reference so does not increment it on construction but
 * does decrement it on destruction.
 *
 * This new reference wrapper can be used as follows:

void function() {
    NewRef(PyLongFromLong(9)); // New reference here.
    // Use static_cast<PyObject*>(NewRef) ...
} // Decrement the new reference here.

 */
class NewRef : public DecRefDtor {
   public:
    NewRef(PyObject *new_ref) : DecRefDtor(new_ref) {}
};

#endif  // __python_utils_hpp__
