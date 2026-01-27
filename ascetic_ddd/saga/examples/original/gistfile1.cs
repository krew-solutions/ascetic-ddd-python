namespace Sagas
{
    using System;
    using System.Collections.Generic;
    

    class Program
    {
        static ActivityHost[] processes;

        static void Main(string[] args)
        {
            var routingSlip = new RoutingSlip(new WorkItem[]
                {
                    new WorkItem<ReserveCarActivity>(new WorkItemArguments{{"vehicleType", "Compact"}}),
                    new WorkItem<ReserveHotelActivity>(new WorkItemArguments{{"roomType", "Suite"}}),
                    new WorkItem<ReserveFlightActivity>(new WorkItemArguments{{"destination", "DUS"}})
                });


            // imagine these being completely separate processes with queues between them
            processes = new ActivityHost[]
                                {
                                    new ActivityHost<ReserveCarActivity>(Send),
                                    new ActivityHost<ReserveHotelActivity>(Send),
                                    new ActivityHost<ReserveFlightActivity>(Send)
                                };

            // hand off to the first address
            Send(routingSlip.ProgressUri, routingSlip);
        }

        static void Send(Uri uri, RoutingSlip routingSlip)
        {
            // this is effectively the network dispatch
            foreach (var process in processes)
            {
                if (process.AcceptMessage(uri, routingSlip))
                {
                    break;
                }
            }
        }

    }


 
    class ReserveCarActivity : Activity
    {
        static Random rnd = new Random(2);

        public override WorkLog DoWork(WorkItem workItem)
        {
            Console.WriteLine("Reserving car");
            var car = workItem.Arguments["vehicleType"];
            var reservationId = rnd.Next(100000);
            Console.WriteLine("Reserved car {0}", reservationId);
            return new WorkLog(this, new WorkResult { { "reservationId", reservationId } });
        }

        public override bool Compensate(WorkLog item, RoutingSlip routingSlip)
        {
            var reservationId = item.Result["reservationId"];
            Console.WriteLine("Cancelled car {0}", reservationId);
            return true;
        }

        public override Uri WorkItemQueueAddress
        {
            get { return new Uri("sb://./carReservations"); }
        }

        public override Uri CompensationQueueAddress
        {
            get { return new Uri("sb://./carCancellactions"); }
        }
    }

    class ReserveHotelActivity : Activity
    {
        static Random rnd = new Random(1);

        public override WorkLog DoWork(WorkItem workItem)
        {
            Console.WriteLine("Reserving hotel");
            var car = workItem.Arguments["roomType"];
            var reservationId = rnd.Next(100000);
            Console.WriteLine("Reserved hotel {0}", reservationId);
            return new WorkLog(this, new WorkResult { { "reservationId", reservationId } });
        }

        public override bool Compensate(WorkLog item, RoutingSlip routingSlip)
        {
            var reservationId = item.Result["reservationId"];
            Console.WriteLine("Cancelled hotel {0}", reservationId);
            return true;
        }

        public override Uri WorkItemQueueAddress
        {
            get { return new Uri("sb://./hotelReservations"); }
        }

        public override Uri CompensationQueueAddress
        {
            get { return new Uri("sb://./hotelCancellations"); }
        }
    }

    class ReserveFlightActivity : Activity
    {
        static Random rnd = new Random(3);

        public override WorkLog DoWork(WorkItem workItem)
        {
            Console.WriteLine("Reserving flight");
            var car = workItem.Arguments["fatzbatz"]; // this throws
            var reservationId = rnd.Next(100000);
            Console.WriteLine("Reserved flight {0}", reservationId);
            return new WorkLog(this, new WorkResult { { "reservationId", reservationId } });
        }

        public override bool Compensate(WorkLog item, RoutingSlip routingSlip)
        {
            var reservationId = item.Result["reservationId"];
            Console.WriteLine("Cancelled flight {0}", reservationId);
            return true;
        }

        public override Uri WorkItemQueueAddress
        {
            get { return new Uri("sb://./flightReservations"); }
        }

        public override Uri CompensationQueueAddress
        {
            get { return new Uri("sb://./flightCancellations"); }
        }
    }


    abstract class Activity
    {
        public abstract WorkLog DoWork(WorkItem item);
        public abstract bool Compensate(WorkLog item, RoutingSlip routingSlip);
        public abstract Uri WorkItemQueueAddress { get; }
        public abstract Uri CompensationQueueAddress { get; }
    }

    class WorkLog
    {
        readonly Type activityType;
        readonly WorkResult result;

        public WorkLog(Activity activity, WorkResult result)
        {
            this.result = result;
            this.activityType = activity.GetType();
        }

        public WorkResult Result
        {
            get { return this.result; }
        }

        public Type ActivityType
        {
            get { return this.activityType; }
        }
    }

    class WorkItemArguments : Dictionary<string, object>
    {
    }

    class WorkResult : Dictionary<string, object>
    {
    }


    abstract class WorkItem
    {
        protected WorkItem(WorkItemArguments arguments)
        {
            this.Arguments = arguments;
        }

        public RoutingSlip RoutingSlip { get; set; }
        public WorkItemArguments Arguments { get; set; }
        public abstract Type ActivityType { get; }
    }

    class WorkItem<T> : WorkItem where T : Activity
    {
        public WorkItem(WorkItemArguments args) : base(args)
        {
        }

        public override Type ActivityType
        {
            get { return typeof (T); }
        }
    }

    class RoutingSlip
    {
        readonly Stack<WorkLog> completedWorkLogs = new Stack<WorkLog>();
        readonly Queue<WorkItem> nextWorkItem = new Queue<WorkItem>();

        public RoutingSlip()
        {
        }

        public RoutingSlip(IEnumerable<WorkItem> workItems)
        {
            foreach (var workItem in workItems)
            {
                this.nextWorkItem.Enqueue(workItem);
            }
        }

        public bool IsCompleted
        {
            get { return this.nextWorkItem.Count == 0; }
        }

        public bool IsInProgress
        {
            get { return this.completedWorkLogs.Count > 0; }
        }

        public bool ProcessNext()
        {
            if (this.IsCompleted)
            {
                throw new InvalidOperationException();
            }

            var currentItem = this.nextWorkItem.Dequeue();
            var activity = (Activity) Activator.CreateInstance(currentItem.ActivityType);
            try
            {
                var result = activity.DoWork(currentItem);
                if (result != null)
                {
                    this.completedWorkLogs.Push(result);
                    return true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception {0}", e.Message);
            }
            return false;
        }

        public Uri ProgressUri
        {
            get
            {
                if (IsCompleted)
                {
                    return null;
                }
                else
                {
                    return
                        ((Activity) Activator.CreateInstance(this.nextWorkItem.Peek().ActivityType)).
                            WorkItemQueueAddress;
                }
            }
        }

        public Uri CompensationUri
        {
            get
            {
                if (!IsInProgress)
                {
                    return null;
                }
                else
                {
                    return
                        ((Activity) Activator.CreateInstance(this.completedWorkLogs.Peek().ActivityType)).
                            CompensationQueueAddress;
                }
            }
        }

        public bool UndoLast()
        {
            if (!this.IsInProgress)
            {
                throw new InvalidOperationException();
            }

            var currentItem = this.completedWorkLogs.Pop();
            var activity = (Activity) Activator.CreateInstance(currentItem.ActivityType);
            try
            {
                return activity.Compensate(currentItem, this);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception {0}", e.Message);
                throw;
            }
            
        }
    }

    abstract class ActivityHost
    {
        Action<Uri, RoutingSlip> send;

        public ActivityHost(Action<Uri, RoutingSlip> send)
        {
            this.send = send;
        }

        public void ProcessForwardMessage(RoutingSlip routingSlip)
        {
            if (!routingSlip.IsCompleted)
            {
                // if the current step is successful, proceed
                // otherwise go to the Unwind path
                if (routingSlip.ProcessNext())
                {
                    // recursion stands for passing context via message
                    // the routing slip can be fully serialized and passed
                    // between systems. 
                    this.send(routingSlip.ProgressUri, routingSlip);
                }
                else
                {
                    // pass message to unwind message route
                    this.send(routingSlip.CompensationUri, routingSlip);
                }
            }
        }

        public void ProcessBackwardMessage(RoutingSlip routingSlip)
        {
            if (routingSlip.IsInProgress)
            {
                // UndoLast can put new work on the routing slip
                // and return false to go back on the forward 
                // path
                if (routingSlip.UndoLast())
                {
                    // recursion stands for passing context via message
                    // the routing slip can be fully serialized and passed
                    // between systems 
                    this.send(routingSlip.CompensationUri, routingSlip);
                }
                else
                {
                    this.send(routingSlip.ProgressUri, routingSlip);
                }
            }
        }

        public abstract bool AcceptMessage(Uri uri, RoutingSlip routingSlip);
    }

    class ActivityHost<T> : ActivityHost where T : Activity, new()
    {
        public ActivityHost(Action<Uri, RoutingSlip> send)
            : base(send)
        {
        }

        public override bool AcceptMessage(Uri uri, RoutingSlip routingSlip)
        {
            var activity = new T();
            if (activity.CompensationQueueAddress.Equals(uri))
            {
                this.ProcessBackwardMessage(routingSlip);
                return true;
            }
            if (activity.WorkItemQueueAddress.Equals(uri))
            {
                this.ProcessForwardMessage(routingSlip);
                return true;
            }
            return false;
        }

    }
    
}