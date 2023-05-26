//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 矩阵类型定义了矩阵运算的通用接口。
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   * 添加实现
   *
   * Construct a new Matrix instance.
     构造一个新的矩阵实例
   * @param rows The number of rows
    行数
   * @param cols The number of columns
    列数
   *
   */
  Matrix(int rows, int cols) : rows_(rows), cols_(cols) {
        // rows_ = rows;
        // cols_ = cols;
        linear_ = new T[rows * cols]; 
        // linear_ = new T[rows * cols]; 
     }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   * TODO（P0）：在构造函数中分配数组。
   * TODO（P0）：取消分配析构函数中的数组。
   * 包含矩阵元素的扁平数组。 
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix 
      矩阵中的行数*/
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix 
      矩阵中的列数*/
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   * 获取第（i，j）个矩阵元素。
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   * 如果任一索引超出范围，则抛出OUT_OF_RANGE。
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   * 设置第（i，j）个矩阵元素。
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   * 从“source”填充矩阵的元素。
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * 销毁矩阵实例。
   * TODO(P0): Add implementation 
   * 添加实现
   */
  virtual ~Matrix() {
        delete[] linear_; 
        // delete[] linear_;
      }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 * RowMatrix类型是一个具体的矩阵实现。它实现了由矩阵类型定义的接口。
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
     创建构造函数
     不要忘记使用析构函数 delete[] data_
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols){
      //data_ 一个2D数组，包含行主格式的矩阵元素
      data_ = new T *[rows];   //行指针数组 
      for(int i = 0; i < rows; i++){
        data_[i] = Matrix<T>::linear_ + i * cols;  //linear = 1,2,3,4,5,6,7,8,9  rows = 3  cols = 3
    }                                              //cols = 0,1,2  数组首地址 + i * cols * sizeof(T)
    // data_ = new T *[rows];
    // for (int i = 0; i < rows; i++) {
    //   data_[i] = Matrix<T>::linear_ + i * cols;
    // }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix 矩阵中的行数
   */
  int GetRowCount() const override {
      return Matrix<T>::rows_;
      // return Matrix<T>::rows_;
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override {
    return Matrix<T>::cols_;
    // return Matrix<T>::cols_;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   * 
    
   *获取第（i，j）个矩阵元素。
    *
    *如果任一索引超出范围，则抛出OUT_OF_RANGE。
    *
    *@param i行索引
    *@param j列索引
    *@return第（i，j）个矩阵元素
    *如果任一索引超出范围，@throws OUT_OF_RANGE
   */
  T GetElement(int i, int j) const override { 
      if (i < 0 || i >= Matrix<T>::rows_ || j < 0 || j >= Matrix<T>::cols_) {
      // if (i < 0 || i >= Matrix<T>::rows_ || j < 0 || j >= Matrix<T>::cols_) {
        throw Exception(ExceptionType::OUT_OF_RANGE, "index is out of range");
    }
      return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if(i < 0 || i >= Matrix<T>::rows_ || j < 0 || j >= Matrix<T>::cols_){
    // if(i < 0 || i >= Matrix<T>::rows_ || j < 0 || j >= Matrix<T>::cols_){
      throw Exception(ExceptionType::OUT_OF_RANGE, "index is out of range");
    }
    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   * 
   
      *TODO（P0）：添加实现
      *
      *从“source”填充矩阵的元素。
      *
      *在`source的事件中抛出OUT_OF_RANGE`
      *不包含所需数量的元素。
      *
      *@param source源容器
      *如果“源”大小不正确，@throws OUT_OF_RANGE
    */
   
  void FillFrom(const std::vector<T> &source) override {
    int size = static_cast<int>(source.size());
    // int size = static_cast<int>(source.size());
    if(size != Matrix<T>::rows_ * Matrix<T>::cols_){
    // if(size != Matrix<T>::rows_ * Matrix<T>::cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "source is incorrect size");
    }
    for(int k = 0; k < size; k++){
      Matrix<T>::linear_[k] = source[k];
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
        delete[] data_; 
     }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.

   *一个2D数组，包含行主格式的矩阵元素。
   *TODO（P0）：
   *-在构造函数中分配行指针数组。
   *-使用这些指针指向“线性”数组的相应元素。
   *-不要忘记取消分配析构函数中的数组。
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 * 
 * *RowMatrixOperations类定义操作
 * 这可以在“RowMatrix”的实例上执行。
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    if (matrixA->GetRowCount() != matrixB->GetRowCount() || matrixA->GetColumnCount() != matrixA->GetColumnCount()) {
      return {};
    }
    int rows = matrixA->GetRowCount();
    int cols = matrixA->GetColumnCount();
    T sum;
    //std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(rows, cols));
    auto mat_c = std::unique_ptr<RowMatrix<T>>(std::make_unique<RowMatrix<T>>(rows, cols));
    //auto mat_c(std::make_unique<RowMatrix<T>>(rows, cols));
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        sum = matrixA->GetElement(i, j) + matrixB->GetElement(i, j);
        mat_c->SetElement(i, j, sum);
      }
    }
    return mat_c;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    // if (matrixA->GetRowCount() != matrixB->GetRowCount() || matrixA->GetColumnCount() != matrixA->GetColumnCount()) {
    if (matrixA->GetColumnCount() != matrixB->GetRowCount()) {    
      return {};
    }
    int rows = matrixA->GetRowCount();  // A 的行数
    // int cols = matrixA->GetColumnCount();
    int cols = matrixB->GetColumnCount();  // B 的列数
    int k_size = matrixA->GetColumnCount();  // A的列数， B的行数
    T sum;
    auto mat_c = std::unique_ptr<RowMatrix<T>>(std::make_unique<RowMatrix<T>>(rows, cols));
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        sum = 0;
        for (int k = 0; k < k_size; k++) {
          // sum += matrixA->GetElement(i, k) * matrixA->GetElement(k, j);
          sum += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
        }
        mat_c->SetElement(i, j, sum);
      }
    }
    return mat_c;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   * 
   * *简化的通用矩阵乘法运算。计算（`matrixA`*`matrixB`+`matrixC`）。
    *如果输入矩阵的维度不匹配，则返回“nullptr”
    *@param matrixA输入矩阵
    *@param matrixB输入矩阵
    *@param matrixC输入矩阵
    *@return一般矩阵相乘的结果
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    auto martrix_tmp = Multiply(matrixA, matrixB);
    if (martrix_tmp != nullptr) {
      return Add(martrix_tmp.get(), matrixC);
    }
    return {};
    }
  };
} // namespace bustub
