<?php

namespace Graze\DataDb\Adapter;

use ArrayIterator;
use Graze\DataDb\Dialect\DialectInterface;
use Traversable;
use Zend_Db_Adapter_Abstract;

class Zend1Adapter implements AdapterInterface
{
    /** @var Zend_Db_Adapter_Abstract */
    private $zendAdapter;
    /** @var DialectInterface */
    private $dialect;

    /**
     * Zend1Adapter constructor.
     *
     * @param Zend_Db_Adapter_Abstract $zendAdapter
     * @param DialectInterface         $dialect
     */
    public function __construct(Zend_Db_Adapter_Abstract $zendAdapter, DialectInterface $dialect)
    {
        $this->zendAdapter = $zendAdapter;
        $this->dialect = $dialect;
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return mixed
     */
    public function query($sql, array $bind = [])
    {
        return $this->zendAdapter->query($sql, $bind);
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return Traversable
     */
    public function fetch($sql, array $bind = [])
    {
        return new ArrayIterator($this->fetchAll($sql, $bind));
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array
     */
    public function fetchAll($sql, array $bind = [])
    {
        return $this->zendAdapter->fetchAll($sql, $bind);
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array|bool
     */
    public function fetchRow($sql, array $bind = [])
    {
        return $this->zendAdapter->fetchRow($sql, $bind);
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return mixed
     */
    public function fetchOne($sql, array $bind = [])
    {
        return $this->zendAdapter->fetchOne($sql, $bind);
    }

    /**
     * @param mixed $value
     *
     * @return string
     */
    public function quoteValue($value)
    {
        return $this->zendAdapter->quoteInto('?', $value);
    }

    /**
     * Start a transaction
     *
     * @return static
     */
    public function beginTransaction()
    {
        $this->zendAdapter->beginTransaction();
        return $this;
    }

    /**
     * @return static
     */
    public function commit()
    {
        $this->zendAdapter->commit();
        return $this;
    }

    /**
     * @return static
     */
    public function rollback()
    {
        $this->zendAdapter->rollBack();
        return $this;
    }

    /**
     * @return DialectInterface
     */
    public function getDialect()
    {
        return $this->dialect;
    }
}
