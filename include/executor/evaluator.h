/**
 * @file evaluator.h
 * @brief Expression evaluator for query execution
 *
 * @defgroup evaluator Expression Evaluator
 * @{
 */

#ifndef SQL_ENGINE_EXECUTOR_EVALUATOR_H
#define SQL_ENGINE_EXECUTOR_EVALUATOR_H

#include <stdbool.h>
#include <stdint.h>

#include "common/types.h"
#include "parser/ast.h"

/**
 * @brief Evaluation context (for expression evaluation)
 */
typedef struct eval_context_t {
    /** Tuple being evaluated */
    tuple_t *tuple;
    
    /** Parameters */
    value_t *params;
    int num_params;
    
    /** Output value */
    value_t *output;
    
    /** Error flag */
    bool error;
    
    /** NULL flag */
    bool is_null;
} eval_context_t;

/**
 * @brief Create evaluation context
 * @param tuple Tuple to evaluate against
 * @return New context or NULL on error
 */
eval_context_t *evaluator_create_context(tuple_t *tuple);

/**
 * @brief Destroy evaluation context
 * @param context Context to destroy
 */
void evaluator_destroy_context(eval_context_t *context);

/**
 * @brief Evaluate an expression
 * @param context Evaluation context
 * @param expr Expression to evaluate
 * @return Result value
 */
value_t evaluator_eval(eval_context_t *context, ast_expression_t *expr);

/**
 * @brief Evaluate a condition (returns boolean)
 * @param context Evaluation context
 * @param expr Expression to evaluate
 * @return true if condition is true
 */
bool evaluator_eval_condition(eval_context_t *context, ast_expression_t *expr);

/**
 * @brief Compare two values
 * @param a First value
 * @param op Comparison operator
 * @param b Second value
 * @return true if comparison is true
 */
bool evaluator_compare(value_t *a, comparison_operator_t op, value_t *b);

/**
 * @brief Convert value to boolean
 * @param value Value to convert
 * @return Boolean result
 */
bool evaluator_to_bool(value_t *value);

/**
 * @brief Print value for debugging
 * @param value Value to print
 */
void evaluator_print_value(value_t *value);

/**
 * @brief Copy value
 * @param dest Destination
 * @param src Source
 */
void evaluator_copy_value(value_t *dest, value_t *src);

/**
 * @brief Free value
 * @param value Value to free
 */
void evaluator_free_value(value_t *value);

/**
 * @brief Create NULL value
 * @return NULL value
 */
value_t evaluator_make_null(void);

/**
 * @brief Create integer value
 * @param val Integer value
 * @return Value struct
 */
value_t evaluator_make_int(int64_t val);

/**
 * @brief Create float value
 * @param val Float value
 * @return Value struct
 */
value_t evaluator_make_float(double val);

/**
 * @brief Create string value
 * @param str String value (will be copied)
 * @return Value struct
 */
value_t evaluator_make_string(const char *str);

/**
 * @brief Create boolean value
 * @param val Boolean value
 * @return Value struct
 */
value_t evaluator_make_bool(bool val);

#endif /* SQL_ENGINE_EXECUTOR_EVALUATOR_H */

/** @} */ /* evaluator */
