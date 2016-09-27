<?php

namespace Graze\DataDb\Adapter;

use Graze\DataDb\Dialect\DialectInterface;
use Traversable;

interface AdapterInterface
{
    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return mixed
     */
    public function query($sql, array $bind = []);

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return Traversable
     */
    public function fetch($sql, array $bind = []);

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array collection of rows or empty array for no results
     */
    public function fetchAll($sql, array $bind = []);

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array|bool single row or false for no result
     */
    public function fetchRow($sql, array $bind = []);

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return mixed single value or false for no result
     */
    public function fetchOne($sql, array $bind = []);

    /**
     * Add value quotes around a specified value
     *
     * @param mixed $value
     *
     * @return string
     */
    public function quoteValue($value);

    /**
     * Start a transaction
     *
     * @return static
     */
    public function beginTransaction();

    /**
     * @return static
     */
    public function commit();

    /**
     * @return static
     */
    public function rollback();

    /**
     * @return DialectInterface
     */
    public function getDialect();
}
