/**
 * @file evaluator.c
 * @brief Expression evaluator implementation
 *
 * @defgroup evaluator Expression Evaluator
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "executor/evaluator.h"
#include "parser/ast.h"

/**
 * @brief Create evaluation context
 */
eval_context_t *evaluator_create_context(tuple_t *tuple) {
    eval_context_t *context;
    
    context = ALLOC_ZERO(sizeof(eval_context_t));
    if (context == NULL) {
        return NULL;
    }
    
    context->tuple = tuple;
    context->params = NULL;
    context->num_params = 0;
    context->output = ALLOC_ZERO(sizeof(value_t));
    context->error = false;
    context->is_null = false;
    
    return context;
}

/**
 * @brief Destroy evaluation context
 */
void evaluator_destroy_context(eval_context_t *context) {
    if (context == NULL) {
        return;
    }
    
    FREE(context->output);
    FREE(context);
}

/**
 * @brief Evaluate a column reference
 */
static value_t eval_column(eval_context_t *context, ast_expression_t *expr) {
    value_t result = {0};
    
    if (context->tuple == NULL || expr == NULL) {
        result.type = TYPE_NULL;
        result.is_null = true;
        return result;
    }
    
    /* TODO: Match column name to tuple column */
    /* Placeholder: return first column */
    if (context->tuple->num_attrs > 0) {
        return context->tuple->values[0];
    }
    
    result.type = TYPE_NULL;
    result.is_null = true;
    return result;
}

/**
 * @brief Evaluate a constant
 */
static value_t eval_constant(ast_expression_t *expr) {
    value_t result = {0};
    
    if (expr == NULL) {
        result.type = TYPE_NULL;
        result.is_null = true;
        return result;
    }
    
    result = expr->value;
    
    /* Copy string values */
    if (result.type == TYPE_TEXT || result.type == TYPE_VARCHAR) {
        if (result.value.string_val != NULL) {
            result.value.string_val = STRDUP(result.value.string_val);
        }
    }
    
    return result;
}

/**
 * @brief Evaluate a function call
 */
static value_t eval_function(eval_context_t *context, ast_expression_t *expr) {
    value_t result = {0};
    value_t *args;
    int num_args;
    
    if (expr == NULL || expr->func_name == NULL) {
        result.type = TYPE_NULL;
        result.is_null = true;
        return result;
    }
    
    /* Evaluate arguments */
    args = ALLOC(sizeof(value_t) * expr->num_args);
    for (int i = 0; i < expr->num_args; i++) {
        args[i] = evaluator_eval(context, expr->func_args[i]);
    }
    num_args = expr->num_args;
    
    /* Process function */
    if (strcasecmp(expr->func_name, "COUNT") == 0) {
        result.type = TYPE_INT64;
        result.value.int_val = num_args > 0 ? 1 : 0;
    } else if (strcasecmp(expr->func_name, "SUM") == 0) {
        if (num_args > 0) {
            result = args[0];
        } else {
            result.type = TYPE_NULL;
            result.is_null = true;
        }
    } else if (strcasecmp(expr->func_name, "AVG") == 0) {
        if (num_args > 0) {
            result = args[0];
            result.type = TYPE_FLOAT64;
        } else {
            result.type = TYPE_NULL;
            result.is_null = true;
        }
    } else if (strcasecmp(expr->func_name, "MIN") == 0) {
        if (num_args > 0) {
            result = args[0];
        } else {
            result.type = TYPE_NULL;
            result.is_null = true;
        }
    } else if (strcasecmp(expr->func_name, "MAX") == 0) {
        if (num_args > 0) {
            result = args[0];
        } else {
            result.type = TYPE_NULL;
            result.is_null = true;
        }
    } else if (strcasecmp(expr->func_name, "ABS") == 0) {
        if (num_args > 0 && args[0].type == TYPE_FLOAT64) {
            result = args[0];
            result.value.float64_val = fabs(result.value.float64_val);
        } else if (num_args > 0 && args[0].type == TYPE_INT64) {
            result = args[0];
            result.value.int_val = llabs(result.value.int_val);
        } else {
            result.type = TYPE_NULL;
            result.is_null = true;
        }
    } else if (strcasecmp(expr->func_name, "UPPER") == 0) {
        if (num_args > 0 && args[0].type == TYPE_TEXT) {
            result.type = TYPE_TEXT;
            result.value.string_val = STRDUP(args[0].value.string_val);
            for (char *p = result.value.string_val; *p; p++) {
                *p = toupper(*p);
            }
        } else {
            result.type = TYPE_NULL;
            result.is_null = true;
        }
    } else if (strcasecmp(expr->func_name, "LOWER") == 0) {
        if (num_args > 0 && args[0].type == TYPE_TEXT) {
            result.type = TYPE_TEXT;
            result.value.string_val = STRDUP(args[0].value.string_val);
            for (char *p = result.value.string_val; *p; p++) {
                *p = tolower(*p);
            }
        } else {
            result.type = TYPE_NULL;
            result.is_null = true;
        }
    } else {
        /* Unknown function */
        result.type = TYPE_NULL;
        result.is_null = true;
    }
    
    /* Free arguments */
    for (int i = 0; i < num_args; i++) {
        evaluator_free_value(&args[i]);
    }
    FREE(args);
    
    return result;
}

/**
 * @brief Apply binary operator
 */
static value_t apply_binary_op(value_t *a, token_type_t op, value_t *b) {
    value_t result = {0};
    
    if (a == NULL || b == NULL) {
        result.type = TYPE_NULL;
        result.is_null = true;
        return result;
    }
    
    /* Handle NULL */
    if (a->is_null || b->is_null) {
        result.type = TYPE_NULL;
        result.is_null = true;
        return result;
    }
    
    switch (op) {
        case TOKEN_PLUS:
            if (a->type == TYPE_INT64 && b->type == TYPE_INT64) {
                result.type = TYPE_INT64;
                result.value.int_val = a->value.int_val + b->value.int_val;
            } else {
                result.type = TYPE_FLOAT64;
                double av = a->type == TYPE_INT64 ? a->value.int_val : a->value.float64_val;
                double bv = b->type == TYPE_INT64 ? b->value.int_val : b->value.float64_val;
                result.value.float64_val = av + bv;
            }
            break;
            
        case TOKEN_MINUS:
            if (a->type == TYPE_INT64 && b->type == TYPE_INT64) {
                result.type = TYPE_INT64;
                result.value.int_val = a->value.int_val - b->value.int_val;
            } else {
                result.type = TYPE_FLOAT64;
                double av = a->type == TYPE_INT64 ? a->value.int_val : a->value.float64_val;
                double bv = b->type == TYPE_INT64 ? b->value.int_val : b->value.float64_val;
                result.value.float64_val = av - bv;
            }
            break;
            
        case TOKEN_STAR:
            if (a->type == TYPE_INT64 && b->type == TYPE_INT64) {
                result.type = TYPE_INT64;
                result.value.int_val = a->value.int_val * b->value.int_val;
            } else {
                result.type = TYPE_FLOAT64;
                double av = a->type == TYPE_INT64 ? a->value.int_val : a->value.float64_val;
                double bv = b->type == TYPE_INT64 ? b->value.int_val : b->value.float64_val;
                result.value.float64_val = av * bv;
            }
            break;
            
        case TOKEN_SLASH:
            if (b->type == TYPE_INT64 && b->value.int_val == 0) {
                result.type = TYPE_NULL;
                result.is_null = true;
            } else if (b->type == TYPE_FLOAT64 && b->value.float64_val == 0.0) {
                result.type = TYPE_NULL;
                result.is_null = true;
            } else {
                result.type = TYPE_FLOAT64;
                double av = a->type == TYPE_INT64 ? a->value.int_val : a->value.float64_val;
                double bv = b->type == TYPE_INT64 ? b->value.int_val : b->value.float64_val;
                result.value.float64_val = av / bv;
            }
            break;
            
        case TOKEN_PERCENT:
            if (a->type == TYPE_INT64 && b->type == TYPE_INT64) {
                result.type = TYPE_INT64;
                result.value.int_val = a->value.int_val % b->value.int_val;
            } else {
                result.type = TYPE_NULL;
                result.is_null = true;
            }
            break;
            
        case TOKEN_CONCAT:
            result.type = TYPE_TEXT;
            /* TODO: Implement concatenation */
            result.value.string_val = NULL;
            break;
            
        default:
            result.type = TYPE_NULL;
            result.is_null = true;
    }
    
    return result;
}

/**
 * @brief Evaluate an expression
 */
value_t evaluator_eval(eval_context_t *context, ast_expression_t *expr) {
    value_t result = {0};
    value_t left, right;
    
    if (expr == NULL) {
        result.type = TYPE_NULL;
        result.is_null = true;
        return result;
    }
    
    switch (expr->type) {
        case EXPR_COLUMN:
            return eval_column(context, expr);
            
        case EXPR_CONSTANT:
            return eval_constant(expr);
            
        case EXPR_FUNCTION:
            return eval_function(context, expr);
            
        case EXPR_BINARY:
            left = evaluator_eval(context, expr->left);
            right = evaluator_eval(context, expr->right);
            result = apply_binary_op(&left, expr->op, &right);
            evaluator_free_value(&left);
            evaluator_free_value(&right);
            break;
            
        case EXPR_UNARY:
            if (expr->op == TOKEN_MINUS) {
                result = evaluator_eval(context, expr->expr);
                if (result.type == TYPE_INT64) {
                    result.value.int_val = -result.value.int_val;
                } else if (result.type == TYPE_FLOAT64) {
                    result.value.float64_val = -result.value.float64_val;
                }
            } else if (expr->op == TOKEN_PLUS) {
                result = evaluator_eval(context, expr->expr);
            } else if (expr->op == TOKEN_NOT) {
                result = evaluator_eval(context, expr->expr);
                result.is_null = false;
                if (result.type == TYPE_BOOL) {
                    result.value.bool_val = !result.value.bool_val;
                } else {
                    result.type = TYPE_BOOL;
                    result.value.bool_val = false;
                }
            }
            break;
            
        default:
            result.type = TYPE_NULL;
            result.is_null = true;
    }
    
    return result;
}

/**
 * @brief Evaluate a condition
 */
bool evaluator_eval_condition(eval_context_t *context, ast_expression_t *expr) {
    value_t result;
    
    if (expr == NULL) {
        return true;
    }
    
    if (expr->type == EXPR_BINARY) {
        value_t left = evaluator_eval(context, expr->left);
        value_t right = evaluator_eval(context, expr->right);
        bool cmp_result = evaluator_compare(&left, expr->op, &right);
        evaluator_free_value(&left);
        evaluator_free_value(&right);
        return cmp_result;
    }
    
    if (expr->type == EXPR_UNARY && expr->op == TOKEN_NOT) {
        return !evaluator_eval_condition(context, expr->expr);
    }
    
    result = evaluator_eval(context, expr);
    bool bool_result = evaluator_to_bool(&result);
    evaluator_free_value(&result);
    
    return bool_result;
}

/**
 * @brief Compare two values
 */
bool evaluator_compare(value_t *a, comparison_operator_t op, value_t *b) {
    if (a == NULL || b == NULL) {
        return op == CMP_IS_NULL;
    }
    
    if (a->is_null || b->is_null) {
        return op == CMP_IS_NULL;
    }
    
    /* Type-specific comparison */
    switch (a->type) {
        case TYPE_INT64:
            switch (op) {
                case CMP_EQ: return a->value.int_val == b->value.int_val;
                case CMP_NE: return a->value.int_val != b->value.int_val;
                case CMP_LT: return a->value.int_val < b->value.int_val;
                case CMP_LE: return a->value.int_val <= b->value.int_val;
                case CMP_GT: return a->value.int_val > b->value.int_val;
                case CMP_GE: return a->value.int_val >= b->value.int_val;
                default: return false;
            }
            
        case TYPE_FLOAT64:
            switch (op) {
                case CMP_EQ: return a->value.float64_val == b->value.float64_val;
                case CMP_NE: return a->value.float64_val != b->value.float64_val;
                case CMP_LT: return a->value.float64_val < b->value.float64_val;
                case CMP_LE: return a->value.float64_val <= b->value.float64_val;
                case CMP_GT: return a->value.float64_val > b->value.float64_val;
                case CMP_GE: return a->value.float64_val >= b->value.float64_val;
                default: return false;
            }
            
        case TYPE_TEXT:
        case TYPE_VARCHAR:
            switch (op) {
                case CMP_EQ: return strcmp(a->value.string_val, b->value.string_val) == 0;
                case CMP_NE: return strcmp(a->value.string_val, b->value.string_val) != 0;
                case CMP_LT: return strcmp(a->value.string_val, b->value.string_val) < 0;
                case CMP_LE: return strcmp(a->value.string_val, b->value.string_val) <= 0;
                case CMP_GT: return strcmp(a->value.string_val, b->value.string_val) > 0;
                case CMP_GE: return strcmp(a->value.string_val, b->value.string_val) >= 0;
                default: return false;
            }
            
        default:
            return false;
    }
}

/**
 * @brief Convert value to boolean
 */
bool evaluator_to_bool(value_t *value) {
    if (value == NULL || value->is_null) {
        return false;
    }
    
    switch (value->type) {
        case TYPE_BOOL:
            return value->value.bool_val;
        case TYPE_INT64:
            return value->value.int_val != 0;
        case TYPE_FLOAT64:
            return value->value.float64_val != 0.0;
        case TYPE_TEXT:
        case TYPE_VARCHAR:
            return value->value.string_val != NULL && 
                   strlen(value->value.string_val) > 0;
        default:
            return false;
    }
}

/**
 * @brief Print value for debugging
 */
void evaluator_print_value(value_t *value) {
    if (value == NULL || value->is_null) {
        printf("NULL");
        return;
    }
    
    switch (value->type) {
        case TYPE_BOOL:
            printf("%s", value->value.bool_val ? "TRUE" : "FALSE");
            break;
        case TYPE_INT64:
            printf("%ld", value->value.int_val);
            break;
        case TYPE_FLOAT64:
            printf("%f", value->value.float64_val);
            break;
        case TYPE_TEXT:
        case TYPE_VARCHAR:
            printf("'%s'", value->value.string_val);
            break;
        default:
            printf("UNKNOWN");
    }
}

/**
 * @brief Copy value
 */
void evaluator_copy_value(value_t *dest, value_t *src) {
    if (dest == NULL || src == NULL) {
        return;
    }
    
    *dest = *src;
    
    if (src->type == TYPE_TEXT || src->type == TYPE_VARCHAR) {
        if (src->value.string_val != NULL) {
            dest->value.string_val = STRDUP(src->value.string_val);
        }
    }
}

/**
 * @brief Free value
 */
void evaluator_free_value(value_t *value) {
    if (value == NULL) {
        return;
    }
    
    if (value->type == TYPE_TEXT || value->type == TYPE_VARCHAR) {
        if (value->value.string_val != NULL) {
            FREE(value->value.string_val);
            value->value.string_val = NULL;
        }
    }
    
    value->is_null = true;
}

/**
 * @brief Create NULL value
 */
value_t evaluator_make_null(void) {
    value_t value = {0};
    value.type = TYPE_NULL;
    value.is_null = true;
    return value;
}

/**
 * @brief Create integer value
 */
value_t evaluator_make_int(int64_t val) {
    value_t value = {0};
    value.type = TYPE_INT64;
    value.value.int_val = val;
    value.is_null = false;
    return value;
}

/**
 * @brief Create float value
 */
value_t evaluator_make_float(double val) {
    value_t value = {0};
    value.type = TYPE_FLOAT64;
    value.value.float64_val = val;
    value.is_null = false;
    return value;
}

/**
 * @brief Create string value
 */
value_t evaluator_make_string(const char *str) {
    value_t value = {0};
    value.type = TYPE_TEXT;
    value.is_null = false;
    if (str != NULL) {
        value.value.string_val = STRDUP(str);
    } else {
        value.value.string_val = NULL;
    }
    return value;
}

/**
 * @brief Create boolean value
 */
value_t evaluator_make_bool(bool val) {
    value_t value = {0};
    value.type = TYPE_BOOL;
    value.value.bool_val = val;
    value.is_null = false;
    return value;
}

/** @} */ /* evaluator */
