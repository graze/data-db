<?php

namespace Graze\DataDb\Adapter;

use Graze\DataDb\Dialect\DialectInterface;
use Traversable;
use Zend\Db\Adapter\Adapter;

class Zend2Adapter implements AdapterInterface
{
    /** @var Adapter */
    private $zendAdapter;
    /** @var DialectInterface */
    private $dialect;

    /**
     * Zend1Adapter constructor.
     *
     * @param Adapter          $zendAdapter
     * @param DialectInterface $dialect
     */
    public function __construct(Adapter $zendAdapter, DialectInterface $dialect)
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
        return $this->zendAdapter->query($sql, $bind);
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array
     */
    public function fetchAll($sql, array $bind = [])
    {
        return iterator_to_array($this->fetch($sql, $bind), true);
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array
     */
    public function fetchRow($sql, array $bind = [])
    {
        $results = $this->zendAdapter->query($sql, $bind);
        return $results->current() ?: false;
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return mixed
     */
    public function fetchOne($sql, array $bind = [])
    {
        $row = $this->fetchRow($sql, $bind);
        if ($row) {
            return $row[0];
        } else {
            return false;
        }
    }

    /**
     * @param mixed $value
     *
     * @return string
     */
    public function quoteValue($value)
    {
        return $this->zendAdapter->getPlatform()->quoteValue($value);
    }

    /**
     * Start a transaction
     *
     * @return static
     */
    public function beginTransaction()
    {
        $this->zendAdapter->getDriver()->getConnection()->beginTransaction();
        return $this;
    }

    /**
     * @return static
     */
    public function commit()
    {
        $this->zendAdapter->getDriver()->getConnection()->commit();
        return $this;
    }

    /**
     * @return static
     */
    public function rollback()
    {
        $this->zendAdapter->getDriver()->getConnection()->rollback();
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
