<?php

namespace Graze\DataDb\Adapter;

use Graze\DataDb\Dialect\DialectInterface;
use PDO;
use PDOStatement;
use Traversable;

class PdoAdapter implements AdapterInterface
{
    /** @var PDO */
    private $pdo;
    /** @var DialectInterface */
    private $dialect;

    /**
     * @param PDO              $pdo
     * @param DialectInterface $dialect
     */
    public function __construct(PDO $pdo, DialectInterface $dialect)
    {
        $this->pdo = $pdo;
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
        $statement = $this->pdo->prepare($sql);
        return $statement->execute($bind);
    }

    /**
     * @param string $sql
     * @param array  $bind
     * @param array  $options Extra set of options to pass to prepare
     *
     * @return PDOStatement
     */
    private function prepareQuery($sql, array $bind = [], array $options = [])
    {
        $statement = $this->pdo->prepare($sql, $options);
        foreach ($bind as $key => $value) {
            $statement->bindValue($key, $value);
        }
        return $statement;
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return Traversable
     */
    public function fetch($sql, array $bind = [])
    {
        $iterator = $this->prepareQuery($sql, $bind, [PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => false]);
        return $iterator;
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array
     */
    public function fetchAll($sql, array $bind = [])
    {
        return $this->prepareQuery($sql, $bind)->fetchAll();
    }

    /**
     * @param string $sql
     * @param array  $bind
     *
     * @return array|bool
     */
    public function fetchRow($sql, array $bind = [])
    {
        return $this->prepareQuery($sql, $bind)->fetch();
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
        }

        return false;
    }

    /**
     * @param mixed $value
     *
     * @return string
     */
    public function quoteValue($value)
    {
        return $this->pdo->quote($value);
    }

    /**
     * Start a transaction
     *
     * @return static
     */
    public function beginTransaction()
    {
        $this->pdo->beginTransaction();
        return $this;
    }

    /**
     * @return static
     */
    public function commit()
    {
        $this->pdo->commit();
        return $this;
    }

    /**
     * @return static
     */
    public function rollback()
    {
        $this->pdo->rollBack();
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
