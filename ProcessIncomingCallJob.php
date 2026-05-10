<?php

namespace App\Jobs\Integration;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Throwable;

class ProcessIncomingCallJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public $tries = 5;

    private $callId;

    public function __construct(int $callId)
    {
        $this->callId = $callId;
    }

    /**
     * @throws Throwable
     */
    public function handle(TelephonyClient $telephonyClient): void
    {
        $call = null;
        $operator = null;

        DB::transaction(function () use (&$call, &$operator) {

            $call = Call::where('id', $this->callId)->lockForUpdate()->first();
            if (!$call) {
                throw new NoIncomingCallsException();
            }

            $client = Client::where('phone', $call->phone)->first();
            if ($client) {
                $call->client_id = $client->id;
            }

            $operator = Operator::where('available', true)
                ->orderBy('last_call_at')
                ->lockForUpdate()
                ->first();

            if (!$operator) {
                throw new NoOperatorsException();
            }

            $affected = Operator::where('id', $operator->id)
                ->where('available', true)
                ->update([
                    'available' => false,
                    'last_call_at' => now(),
                ]);

            if ($affected === 0) {
                throw new RetryException();
            }

            $call->operator_id = $operator->id;
            $call->status = 'assigned';
            $call->save();
        });

        // HTTP-запрос во внешнюю телефонию для назначения звонка оператору.
        // Гарантии внешней системы неизвестны.
        try {

            $telephonyClient->sendCallAssigned($call->id, $operator->id);

            logger()->info('Call assigned', [
                'call_id' => $call->id,
                'operator_id' => $operator->id,
            ]);

        } catch (Throwable $e) {

            logger()->error('Telephony call assignment failed', [
                'call_id' => $call->id,
                'operator_id' => $operator->id,
                'error' => $e->getMessage(),
            ]);

            throw $e;
        }
    }
}
